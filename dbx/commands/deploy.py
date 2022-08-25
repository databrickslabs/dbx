import json
import shutil
import tempfile
from pathlib import Path
from typing import Dict, Any, Union, Optional
from typing import List

import mlflow
import typer
from databricks_cli.jobs.api import JobsService, JobsApi
from databricks_cli.sdk.api_client import ApiClient
from requests.exceptions import HTTPError

from dbx.api.config_reader import ConfigReader
from dbx.models.deployment import EnvironmentDeploymentInfo
from dbx.options import (
    DEPLOYMENT_FILE_OPTION,
    ENVIRONMENT_OPTION,
    JINJA_VARIABLES_FILE_OPTION,
    REQUIREMENTS_FILE_OPTION,
    NO_REBUILD_OPTION,
    NO_PACKAGE_OPTION,
    TAGS_OPTION,
    BRANCH_NAME_OPTION,
    DEBUG_OPTION,
    WORKFLOW_ARGUMENT,
)
from dbx.utils import dbx_echo
from dbx.utils.adjuster import adjust_job_definitions
from dbx.utils.common import (
    prepare_environment,
    parse_multiple,
    get_current_branch_name,
)
from dbx.utils.dependency_manager import DependencyManager
from dbx.utils.file_uploader import MlflowFileUploader
from dbx.utils.job_listing import find_job_by_name


def deploy(
    workflow_name: str = WORKFLOW_ARGUMENT,
    deployment_file: Path = DEPLOYMENT_FILE_OPTION,
    job: Optional[str] = typer.Option(
        None,
        "--job",
        help="This option is deprecated, please use workflow name as argument instead.",
        show_default=False,
    ),
    jobs: Optional[str] = typer.Option(
        None, "--jobs", help="This option is deprecated, please use `--workflows` instead.", show_default=False
    ),
    workflows: Optional[str] = typer.Option(
        None, "--workflows", help="Comma-separated list of workflow names to be deployed", show_default=False
    ),
    requirements_file: Optional[Path] = REQUIREMENTS_FILE_OPTION,
    tags: Optional[List[str]] = TAGS_OPTION,
    environment: str = ENVIRONMENT_OPTION,
    no_rebuild: bool = NO_REBUILD_OPTION,
    no_package: bool = NO_PACKAGE_OPTION,
    files_only: bool = typer.Option(
        False,
        "--files-only",
        is_flag=True,
        help="This option is deprecated, please use `--assets-only` instead.",
    ),
    assets_only: bool = typer.Option(
        False,
        "--assets-only",
        is_flag=True,
        help="""When provided, will **only** upload assets
        (📁 referenced files, 📦 core package and workflow definition) to the artifact storage.

        ⚠️ A workflow(s) won't be created or updated in the Jobs UI.


        This option is intended for CI pipelines and for cases when users don't want to affect the job
        instance in the workspace.


        ⚠️ Please note that the only way to launch a workflow that has been deployed with `--assets-only` option
        is to use the `dbx launch --from-assets`.


        Workflows deployed with this option can only be launched to be used with `dbx launch --from-assets` option.""",
    ),
    write_specs_to_file: Optional[Path] = typer.Option(
        None,
        help="""Writes final job definitions into a given local file.

              Helpful when final representation of a deployed job is needed for other integrations.

              Please note that output file will be overwritten if it exists.""",
        writable=True,
    ),
    branch_name: Optional[str] = BRANCH_NAME_OPTION,
    jinja_variables_file: Optional[Path] = JINJA_VARIABLES_FILE_OPTION,
    debug: Optional[bool] = DEBUG_OPTION,  # noqa
):
    dbx_echo(f"Starting new deployment for environment {environment}")

    api_client = prepare_environment(environment)
    additional_tags = parse_multiple(tags)

    if not branch_name:
        branch_name = get_current_branch_name()

    config_reader = ConfigReader(deployment_file, jinja_variables_file)
    config = config_reader.get_config()

    deployment = config.get_environment(environment, raise_if_not_found=True)

    if workflow_name:
        job = workflow_name

    if workflows:
        jobs = workflows

    requested_jobs = _define_deployable_jobs(job, jobs)

    _preprocess_deployment(deployment, requested_jobs)

    if no_rebuild:
        dbx_echo(
            """[yellow bold]
        Legacy [code]--no-rebuild[/code] flag has been used.
        Please specify build logic in the build section of the deployment file instead.[/yellow bold]"""
        )
        config.build.no_build = True

    dependency_manager = DependencyManager(config.build, no_package, requirements_file)

    _assets_only = assets_only if assets_only else files_only

    with mlflow.start_run() as deployment_run:

        artifact_base_uri = deployment_run.info.artifact_uri
        _file_uploader = MlflowFileUploader(artifact_base_uri)

        adjust_job_definitions(deployment.payload.workflows, dependency_manager, _file_uploader, api_client)

        if not _assets_only:
            dbx_echo("Updating job definitions")
            deployment_data = _create_jobs(deployment.payload.workflows, api_client)
            _log_dbx_file(deployment_data, "deployments.json")

            for job_spec in deployment.payload.workflows:
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

        deployment_spec = deployment.to_spec()

        deployment_tags.update(additional_tags)

        if branch_name:
            deployment_tags["dbx_branch_name"] = branch_name

        if _assets_only:
            deployment_tags["dbx_deploy_type"] = "files_only"

        _log_dbx_file(deployment_spec, "deployment-result.json")

        mlflow.set_tags(deployment_tags)
        dbx_echo(f":sparkles: Deployment for environment {environment} finished successfully")

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


def _preprocess_deployment(deployment: EnvironmentDeploymentInfo, requested_jobs: Union[List[str], None]):
    if not deployment.payload.workflows:
        raise Exception("No jobs provided for deployment")

    deployment.payload.workflows = _preprocess_jobs(deployment.payload.workflows, requested_jobs)


def _preprocess_jobs(jobs: List[Dict[str, Any]], requested_jobs: Union[List[str], None]) -> List[Dict[str, Any]]:
    job_names = [job["name"] for job in jobs]
    if requested_jobs:
        dbx_echo(f"Deployment will be performed only for the following jobs: {requested_jobs}")
        for requested_job_name in requested_jobs:
            if requested_job_name not in job_names:
                raise Exception(
                    f"""
                Workflow {requested_job_name} was requested, but not provided in deployment file.
                Available workflows are: {job_names}
                """
                )
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
        dbx_echo(":boom: Failed to create job with definition:")
        dbx_echo(job)
        raise e
    return job_id


def _update_job(jobs_service: JobsService, job_id: str, job: Dict[str, Any]) -> str:
    dbx_echo(f'Updating existing job with id: {job_id} and name: {job["name"]}')
    try:
        jobs_service.reset_job(job_id, job)
    except HTTPError as e:
        dbx_echo(":boom: Failed to update job with definition:")
        dbx_echo(job)
        raise e

    _acl = job.get("access_control_list")
    if _acl:
        _client = jobs_service.client
        _client.perform_query("PUT", f"/permissions/jobs/{job_id}", data={"access_control_list": _acl})

    return job_id
