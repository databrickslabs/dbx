import json
import pathlib
import shutil
import tempfile
from typing import Dict, Any, Union
from typing import List

import click
import mlflow
from databricks_cli.configure.config import debug_option
from databricks_cli.jobs.api import JobsService, JobsApi
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.utils import CONTEXT_SETTINGS
from requests.exceptions import HTTPError

from dbx.utils.common import (
    dbx_echo,
    prepare_environment,
    DEFAULT_DEPLOYMENT_FILE_PATH,
    environment_option,
    parse_multiple,
    FileUploader,
    handle_package,
    get_package_file,
    get_current_branch_name, DeploymentFile,
)


@click.command(
    context_settings=CONTEXT_SETTINGS,
    short_help="""Deploy project to artifact storage.""",
    help="""Deploy project to artifact storage.

    This command takes the project in current folder (the :code:`.dbx/project.json` shall exist)
    and performs deployment to the given environment.

    During the deployment, following actions will be performed:
    
    1. Python package will be built and stored in :code:`dist/*` folder (can be disabled via :option:`--no-rebuild`)
    2. |Deployment configuration will be taken for environment
       |from the deployment file, defined in  :option:`--deployment-file` (default: code:`conf.deployment.json`).
    3. Per each job defined in the :option:`--jobs`, all local file references will be checked
    4. Any found file references will be uploaded to MLflow as artifacts of current deployment run
    5. If :option:`--requirements-file` is provided, all requirements will be added to job definition
    6. Wheel file location will be added to the :code:`libraries`. Can be disabled with :option:`--no-package`.
    7. If the job with given name exists, it will be updated, if not - created

    """
)
@click.option(
    "--deployment-file",
    required=False,
    type=str,
    help="Path to deployment file in json format",
    default=DEFAULT_DEPLOYMENT_FILE_PATH,
)
@click.option(
    "--jobs",
    required=False,
    type=str,
    help="""Comma-separated list of job names to be deployed. 
              If not provided, all jobs from the deployment file will be deployed.
              """,
)
@click.option(
    "--requirements-file", required=False, type=str, default="requirements.txt"
)
@click.option("--no-rebuild", is_flag=True, help="Disable package rebuild")
@click.option(
    "--no-package",
    is_flag=True,
    help="Do not add package reference into the job description",
)
@click.option(
    "--tags",
    multiple=True,
    type=str,
    help="""Additional tags for deployment in format (tag_name=tag_value). 
              Option might be repeated multiple times.""",
)
@debug_option
@environment_option
def deploy(
        deployment_file: str,
        jobs: str,
        requirements_file: str,
        tags: List[str],
        environment: str,
        no_rebuild: bool,
        no_package: bool,
):
    dbx_echo(f"Starting new deployment for environment {environment}")

    api_client = prepare_environment(environment)
    additional_tags = parse_multiple(tags)
    branch_name = get_current_branch_name()
    handle_package(no_rebuild)
    package_file = get_package_file()

    _verify_deployment_file(deployment_file)

    deployment_file_controller = DeploymentFile(deployment_file)
    deployment = deployment_file_controller.get_environment(environment)

    if not deployment:
        raise NameError(
            f"""
        Requested environment {environment} is non-existent in the deployment file {deployment_file}.
        Available environments are: {deployment_file_controller.get_all_environment_names()}
        """
        )

    if jobs:
        requested_jobs = jobs.split(",")
    else:
        requested_jobs = None

    requirements_payload = _preprocess_requirements(requirements_file)

    _preprocess_deployment(deployment, requested_jobs)

    _file_uploader = FileUploader(api_client)

    with mlflow.start_run() as deployment_run:

        artifact_base_uri = deployment_run.info.artifact_uri

        if no_package:
            dbx_echo("No package definition will be added into job description")
            package_requirement = []
        else:
            if package_file:
                package_requirement = [{"whl": str(package_file)}]
            else:
                dbx_echo("Package file was not found! Please check your /dist/ folder")
                package_requirement = []

        _adjust_job_definitions(
            deployment["jobs"],
            artifact_base_uri,
            requirements_payload,
            package_requirement,
            _file_uploader,
        )
        deployment_data = _create_jobs(deployment["jobs"], api_client)
        _log_deployments(deployment_data)

        deployment_tags = {
            "dbx_action_type": "deploy",
            "dbx_environment": environment,
            "dbx_status": "SUCCESS",
        }

        deployment_tags.update(additional_tags)
        if branch_name:
            deployment_tags["dbx_branch_name"] = branch_name

        mlflow.set_tags(deployment_tags)
        dbx_echo(f"Deployment for environment {environment} finished successfully")


def _delete_managed_libraries(packages: List[str]) -> List[str]:
    output_packages = []

    for package in packages:

        if package.startswith("pyspark"):
            dbx_echo(
                "pyspark dependency deleted from the list of libraries, because it's a managed library"
            )
        else:
            output_packages.append(package)

    return output_packages


def _preprocess_requirements(requirements):
    requirements_path = pathlib.Path(requirements)

    if not requirements_path.exists():
        dbx_echo("Requirements file is not provided")
        return []
    else:
        requirements_content = requirements_path.read_text().split("\n")
        filtered_libraries = _delete_managed_libraries(requirements_content)

        requirements_payload = [
            {"pypi": {"package": req}} for req in filtered_libraries if req
        ]
        return requirements_payload


def _log_deployments(deployment_data):
    temp_dir = tempfile.mkdtemp()
    serialized_data = json.dumps(deployment_data, indent=4)
    temp_path = pathlib.Path(temp_dir, "deployments.json")
    temp_path.write_text(serialized_data)
    mlflow.log_artifact(str(temp_path), ".dbx")
    shutil.rmtree(temp_dir)


def _verify_deployment_file(deployment_file: str):
    if not deployment_file.endswith(".json"):
        raise Exception("Deployment file should have .json extension")

    if not pathlib.Path(deployment_file).exists():
        raise Exception(f"Deployment {deployment_file} file is non-existent")


def _preprocess_deployment(
        deployment: Dict[str, Any], requested_jobs: Union[List[str], None]
):
    if "jobs" not in deployment:
        raise Exception("No jobs provided for deployment")

    deployment["jobs"] = _preprocess_jobs(deployment["jobs"], requested_jobs)


def _preprocess_files(files: Dict[str, Any]):
    for key, file_path_str in files.items():
        file_path = pathlib.Path(file_path_str)
        if not file_path.exists():
            raise FileNotFoundError(f"File path {file_path} is non-existent")
        files[key] = file_path


def _preprocess_jobs(
        jobs: List[Dict[str, Any]], requested_jobs: Union[List[str], None]
) -> List[Dict[str, Any]]:
    job_names = [job["name"] for job in jobs]
    if requested_jobs:
        dbx_echo(
            f"Deployment will be performed only for the following jobs: {requested_jobs}"
        )
        for requested_job_name in requested_jobs:
            if requested_job_name not in job_names:
                raise Exception(
                    f"Job {requested_job_name} was requested, but not provided in deployment file"
                )
        preprocessed_jobs = [job for job in jobs if job["name"] in requested_jobs]
    else:
        preprocessed_jobs = jobs
    return preprocessed_jobs


def _adjust_job_definitions(
        jobs: List[Dict[str, Any]],
        artifact_base_uri: str,
        requirements_payload: List[Dict[str, str]],
        package_payload: List[Dict[str, str]],
        file_uploader: FileUploader,
):
    def adjustment_callback(p: Any):
        return _adjust_path(p, artifact_base_uri, file_uploader)

    for job in jobs:
        job["libraries"] = job.get("libraries", []) + package_payload
        _walk_content(adjustment_callback, job)
        job["libraries"] = job.get("libraries", []) + requirements_payload


def _create_jobs(jobs: List[Dict[str, Any]], api_client: ApiClient) -> Dict[str, int]:
    deployment_data = {}
    for job in jobs:
        dbx_echo(f'Processing deployment for job: {job["name"]}')
        jobs_service = JobsService(api_client)
        all_jobs = jobs_service.list_jobs().get("jobs", [])
        matching_jobs = [j for j in all_jobs if j["settings"]["name"] == job["name"]]

        if not matching_jobs:
            job_id = _create_job(api_client, job)
        else:

            if len(matching_jobs) > 1:
                raise Exception(
                    f"""There are more than one job with name {job["name"]}. 
                Please delete duplicated jobs first"""
                )

            job_id = matching_jobs[0]["job_id"]
            _update_job(jobs_service, job_id, job)

        deployment_data[job["name"]] = job_id
    return deployment_data


def _create_job(api_client: ApiClient, job: Dict[str, Any]) -> str:
    dbx_echo(f'Creating a new job with name {job["name"]}')
    try:
        jobs_api = JobsApi(api_client)
        job_id = jobs_api.create_job(job)["job_id"]
    except HTTPError as e:
        dbx_echo("Failed to create job with definition:")
        dbx_echo(json.dumps(job, indent=4))
        raise e
    return job_id


def _update_job(jobs_service: JobsService, job_id: str, job: Dict[str, Any]) -> str:
    dbx_echo(f'Updating existing job with id: {job_id} and name: {job["name"]}')
    try:
        jobs_service.reset_job(job_id, job)
    except HTTPError as e:
        dbx_echo("Failed to update job with definition:")
        dbx_echo(json.dumps(job, indent=4))
        raise e
    return job_id


def _walk_content(func, content, parent=None, index=None):
    if isinstance(content, dict):
        for key, item in content.items():
            _walk_content(func, item, content, key)
    elif isinstance(content, list):
        for idx, sub_item in enumerate(content):
            _walk_content(func, sub_item, content, idx)
    else:
        parent[index] = func(content)


def _adjust_path(candidate, adjustment, file_uploader: FileUploader):
    if isinstance(candidate, str):
        # path already adjusted or points to another dbfs object - pass it
        if candidate.startswith("dbfs"):
            return candidate
        else:
            file_path = pathlib.Path(candidate)

            # this is a fix for pathlib behaviour related to WinError
            # in case if we pass incorrect or unsupported string, for example local[*] on Win we receive a OSError
            try:
                local_file_exists = file_path.exists()
            except OSError:
                local_file_exists = False

            if local_file_exists:
                adjusted_path = "%s/%s" % (adjustment, file_path.as_posix())
                if file_uploader.file_exists(adjusted_path):
                    dbx_echo(
                        "File is already stored in the deployment, no action needed"
                    )
                else:
                    file_uploader.upload_file(file_path)
                return adjusted_path
            else:
                return candidate
    else:
        return candidate
