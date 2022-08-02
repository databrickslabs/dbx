import json
from pathlib import Path
import shutil
import tempfile
from typing import Dict, Any, Union, Optional
from typing import List

import click
import mlflow
from databricks_cli.configure.config import debug_option
from databricks_cli.jobs.api import JobsService, JobsApi
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.utils import CONTEXT_SETTINGS
from requests.exceptions import HTTPError

from dbx.api.config_reader import ConfigReader
from dbx.callbacks import verify_jinja_variables_file
from dbx.utils.adjuster import adjust_job_definitions
from dbx.utils.common import (
    prepare_environment,
    parse_multiple,
    get_current_branch_name,
)
from dbx.utils import dbx_echo
from dbx.utils.file_uploader import MlflowFileUploader
from dbx.utils.options import environment_option, deployment_file_option
from dbx.utils.dependency_manager import DependencyManager
from dbx.utils.job_listing import find_job_by_name


@click.command(
    context_settings=CONTEXT_SETTINGS,
    short_help="""Deploy project to artifact storage.""",
    help="""Deploy project to artifact storage.

    This command takes the project in current folder (file :code:`.dbx/project.json` shall exist)
    and performs deployment to the given environment.

    During the deployment, following actions will be performed:

    1. Python package will be built and stored in :code:`dist/*` folder (can be disabled via :option:`--no-rebuild`)
    2. | Deployment configuration will be taken for a given environment (see :option:`-e` for details)
       | from the deployment file, defined in  :option:`--deployment-file`.
       | You can specify the deployment file in either JSON or YAML or Jinja-based JSON or YAML.
       | :code:`[.json, .yaml, .yml, .j2]` are all valid file types.
    3. Per each job defined in the :option:`--jobs`, all local file references will be checked
    4. Any found file references will be uploaded to MLflow as artifacts of current deployment run
    5. [DEPRECATED] If :option:`--requirements-file` is provided, all requirements will be added to job definition
    6. Wheel file location will be added to the :code:`libraries`. Can be disabled with :option:`--no-package`.
    7. If the job with given name exists, it will be updated, if not - created
    8. | If :option:`--write-specs-to-file` is provided, writes final job spec into a given file.
       | For example, this option can look like this: :code:`--write-specs-to-file=.dbx/deployment-result.json`.
    """,
)
@click.option(
    "--job",
    required=False,
    type=str,
    help="""Deploy a single job by it's name.
              Both :code:`--jobs` and :code:`--job` cannot be provided.
              """,
)
@click.option(
    "--jobs",
    required=False,
    type=str,
    help="""Comma-separated list of job names to be deployed.
              If not provided, all jobs from the deployment file will be deployed.
              Both :code:`--jobs` and :code:`--job` cannot be provided.
              """,
)
@click.option(
    "--requirements-file",
    required=False,
    type=click.Path(path_type=Path),
    default=Path("requirements.txt"),
    help="[DEPRECATED]",
)
@click.option("--no-rebuild", is_flag=True, help="Disable package rebuild")
@click.option(
    "--no-package",
    is_flag=True,
    help="Do not add package reference into the job description",
)
@click.option(
    "--files-only",
    is_flag=True,
    help="Do not create jobs, only deploy files.",
)
@click.option(
    "--tags",
    multiple=True,
    type=str,
    help="""Additional tags for deployment in format (tag_name=tag_value).
              Option might be repeated multiple times.""",
)
@click.option(
    "--write-specs-to-file",
    type=str,
    default=None,
    help="""Writes final job definitions into a given local file.
              Helpful when final representation of a deployed job is needed for other integrations.
              Please note that output file will be overwritten if it exists.""",
)
@click.option(
    "--branch-name",
    type=str,
    default=None,
    required=False,
    help="""The name of the current branch.
              If not provided or empty, dbx will try to detect the branch name.""",
)
@click.option(
    "--jinja-variables-file",
    type=click.Path(path_type=Path),
    default=None,
    required=False,
    help="""
        Path to a file with variables for Jinja template. Only works when Jinja-based deployment file is used.
        Read more about this functionality in the Jinja2 support doc.
        """,
    callback=verify_jinja_variables_file,
)
@debug_option
@environment_option
@deployment_file_option
def deploy(
    deployment_file: Optional[Path],
    job: Optional[str],
    jobs: Optional[str],
    requirements_file: Optional[Path],
    tags: List[str],
    environment: str,
    no_rebuild: bool,
    no_package: bool,
    files_only: bool,
    write_specs_to_file: Optional[str],
    branch_name: Optional[str],
    jinja_variables_file: Optional[Path],
):
    dbx_echo(f"Starting new deployment for environment {environment}")

    api_client = prepare_environment(environment)
    additional_tags = parse_multiple(tags)

    if not branch_name:
        branch_name = get_current_branch_name()

    config_reader = ConfigReader(deployment_file, jinja_variables_file)

    deployment = config_reader.get_environment(environment)

    if not deployment:
        raise NameError(
            f"""
        Requested environment {environment} is non-existent in the deployment file {deployment_file}.
        Available environments are: {config_reader.get_all_environment_names()}
        """
        )

    requested_jobs = _define_deployable_jobs(job, jobs)

    _preprocess_deployment(deployment, requested_jobs)

    dependency_manager = DependencyManager(no_package, no_rebuild, requirements_file)

    with mlflow.start_run() as deployment_run:

        artifact_base_uri = deployment_run.info.artifact_uri
        _file_uploader = MlflowFileUploader(artifact_base_uri)

        adjust_job_definitions(deployment["jobs"], dependency_manager, _file_uploader, api_client)

        if not files_only:
            dbx_echo("Updating job definitions")
            deployment_data = _create_jobs(deployment["jobs"], api_client)
            _log_dbx_file(deployment_data, "deployments.json")

            for job_spec in deployment.get("jobs"):
                permissions = job_spec.get("permissions")
                if permissions:
                    job_name = job_spec.get("name")
                    dbx_echo(f"Permission settings are provided for job {job_name}, setting it up")
                    job_id = deployment_data.get(job_spec.get("name"))
                    api_client.perform_query("PUT", f"/permissions/jobs/{job_id}", data=permissions)
                    dbx_echo(f"Permission settings were successfully set for job {job_name}")

            dbx_echo("Updating job definitions - done")

        deployment_tags = {
            "dbx_action_type": "deploy",
            "dbx_environment": environment,
            "dbx_status": "SUCCESS",
        }

        deployment_spec = {environment: deployment}

        deployment_tags.update(additional_tags)

        if branch_name:
            deployment_tags["dbx_branch_name"] = branch_name

        if files_only:
            deployment_tags["dbx_deploy_type"] = "files_only"

        _log_dbx_file(deployment_spec, "deployment-result.json")

        mlflow.set_tags(deployment_tags)
        dbx_echo(f"Deployment for environment {environment} finished successfully :sparkles:")

        if write_specs_to_file:
            dbx_echo("Writing final job specifications into file")
            specs_file = Path(write_specs_to_file)

            if specs_file.exists():
                specs_file.unlink()

            specs_file.write_text(json.dumps(deployment_spec, indent=4), encoding="utf-8")


def _log_dbx_file(content: Dict[Any, Any], name: str):
    temp_dir = tempfile.mkdtemp()
    serialized_data = json.dumps(content, indent=4)
    temp_path = Path(temp_dir, name)
    temp_path.write_text(serialized_data, encoding="utf-8")
    mlflow.log_artifact(str(temp_path), ".dbx")
    shutil.rmtree(temp_dir)


def _define_deployable_jobs(job: str, jobs: str) -> Optional[List[str]]:
    if jobs and job:
        raise Exception("Both --job and --jobs cannot be provided together")

    if job:
        requested_jobs = [job]
    elif jobs:
        requested_jobs = jobs.split(",")
    else:
        requested_jobs = None

    return requested_jobs


def _preprocess_deployment(deployment: Dict[str, Any], requested_jobs: Union[List[str], None]):
    if "jobs" not in deployment:
        raise Exception("No jobs provided for deployment")

    deployment["jobs"] = _preprocess_jobs(deployment["jobs"], requested_jobs)


def _preprocess_jobs(jobs: List[Dict[str, Any]], requested_jobs: Union[List[str], None]) -> List[Dict[str, Any]]:
    job_names = [job["name"] for job in jobs]
    if requested_jobs:
        dbx_echo(f"Deployment will be performed only for the following jobs: {requested_jobs}")
        for requested_job_name in requested_jobs:
            if requested_job_name not in job_names:
                raise Exception(f"Job {requested_job_name} was requested, but not provided in deployment file")
        preprocessed_jobs = [job for job in jobs if job["name"] in requested_jobs]
    else:
        preprocessed_jobs = jobs
    return preprocessed_jobs


def _create_jobs(jobs: List[Dict[str, Any]], api_client: ApiClient) -> Dict[str, int]:
    deployment_data = {}
    for job in jobs:
        dbx_echo(f'Processing deployment for job: {job["name"]}')
        jobs_service = JobsService(api_client)
        matching_job = find_job_by_name(jobs_service, job["name"])

        if not matching_job:
            job_id = _create_job(api_client, job)
        else:
            job_id = matching_job["job_id"]
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
