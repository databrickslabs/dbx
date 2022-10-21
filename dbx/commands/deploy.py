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

from dbx.api.adjuster.adjuster import Adjuster, AdditionalLibrariesProvider
from dbx.api.config_reader import ConfigReader
from dbx.api.dependency.core_package import CorePackageManager
from dbx.api.dependency.requirements import RequirementsFileProcessor
from dbx.models.deployment import EnvironmentDeploymentInfo, WorkflowList, AnyWorkflow
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
from dbx.utils.common import (
    prepare_environment,
    parse_multiple,
    get_current_branch_name,
)
from dbx.utils.file_uploader import MlflowFileUploader
from dbx.utils.job_listing import find_job_by_name


def deploy(
    workflow_name: str = WORKFLOW_ARGUMENT,
    deployment_file: Path = DEPLOYMENT_FILE_OPTION,
    job_name: Optional[str] = typer.Option(
        None,
        "--job",
        help="This option is deprecated, please use workflow name as argument instead.",
        show_default=False,
    ),
    job_names: Optional[str] = typer.Option(
        None, "--jobs", help="This option is deprecated, please use `--workflows` instead.", show_default=False
    ),
    workflow_names: Optional[str] = typer.Option(
        None, "--workflows", help="Comma-separated list of workflow names to be deployed", show_default=False
    ),
    requirements_file: Optional[Path] = REQUIREMENTS_FILE_OPTION,
    tags: Optional[List[str]] = TAGS_OPTION,
    environment_name: str = ENVIRONMENT_OPTION,
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
    dbx_echo(f"Starting new deployment for environment {environment_name}")

    api_client = prepare_environment(environment_name)
    additional_tags = parse_multiple(tags)

    if not branch_name:
        branch_name = get_current_branch_name()

    config_reader = ConfigReader(deployment_file, jinja_variables_file)
    config = config_reader.get_config()

    environment_info = config.get_environment(environment_name, raise_if_not_found=True)

    workflow_name = workflow_name if workflow_name else job_name
    workflow_names = workflow_names.split(",") if workflow_names else job_names.split(",") if job_names else []

    deployable_workflows = environment_info.payload.select_relevant_or_all_workflows(workflow_name, workflow_names)
    environment_info.payload.workflows = deployable_workflows  # filter out the chosen set of workflows
    if no_rebuild:
        dbx_echo(
            """[yellow bold]
        Legacy [code]--no-rebuild[/code] flag has been used.
        Please specify build logic in the build section of the deployment file instead.[/yellow bold]"""
        )
        config.build.no_build = True

    core_package = None if config.build.no_build else CorePackageManager(build_config=config.build).core_package
    libraries_from_requirements = RequirementsFileProcessor(requirements_file).libraries if requirements_file else []

    _assets_only = assets_only if assets_only else files_only

    with mlflow.start_run() as deployment_run:

        adjuster = Adjuster(
            api_client=api_client,
            file_uploader=MlflowFileUploader(deployment_run.info.artifact_uri),
            additional_libraries=AdditionalLibrariesProvider(
                no_package=no_package,
                core_package=core_package,
                libraries_from_requirements=libraries_from_requirements,
            ),
        )
        adjuster.traverse(deployable_workflows)

        if not _assets_only:
            dbx_echo("🤖 Updating workflow definitions via API")
            deployment_data = _create_workflows(deployable_workflows, api_client)
            _log_dbx_file(deployment_data, "deployments.json")

            for workflow in deployable_workflows:
                if workflow.access_control_list:
                    dbx_echo(f"🛂 Applying permission settings for workflow {workflow.name}")
                    api_client.perform_query(
                        "PUT",
                        f"/permissions/jobs/{workflow.job_id}",
                        data=workflow.get_acl_payload(),
                    )
                    dbx_echo(f"✅ Permission settings were successfully set for workflow {workflow.name}")

            dbx_echo("✅ Updating workflow definitions - done")

        deployment_tags = {
            "dbx_action_type": "deploy",
            "dbx_environment": environment_name,
            "dbx_status": "SUCCESS",
        }

        environment_spec = environment_info.to_spec()

        deployment_tags.update(additional_tags)

        if branch_name:
            deployment_tags["dbx_branch_name"] = branch_name

        if _assets_only:
            deployment_tags["dbx_deploy_type"] = "files_only"

        _log_dbx_file(environment_spec, "deployment-result.json")

        mlflow.set_tags(deployment_tags)
        dbx_echo(f":sparkles: Deployment for environment {environment_name} finished successfully")

        if write_specs_to_file:
            dbx_echo("Writing final job specifications into file")
            specs_file = Path(write_specs_to_file)

            if specs_file.exists():
                specs_file.unlink()

            specs_file.write_text(json.dumps(environment_spec, indent=4), encoding="utf-8")


def _log_dbx_file(content: Dict[Any, Any], name: str):
    temp_dir = tempfile.mkdtemp()
    serialized_data = json.dumps(content, indent=4)
    temp_path = Path(temp_dir, name)
    temp_path.write_text(serialized_data, encoding="utf-8")
    mlflow.log_artifact(str(temp_path), ".dbx")
    shutil.rmtree(temp_dir)


def define_workflow_names(workflow_name: str, workflow_names: str) -> Optional[List[str]]:
    if workflow_names and workflow_name:
        raise Exception("Both workflow argument and --workflows (or --job and --jobs) cannot be provided together")

    if workflow_name:
        _workflow_names = [workflow_name]
    elif workflow_names:
        _workflow_names = workflow_names.split(",")
    else:
        _workflow_names = None

    return _workflow_names


def _preprocess_deployment(deployment: EnvironmentDeploymentInfo, requested_workflows: Union[List[str], None]):
    if not deployment.payload.workflows:
        dbx_echo("[yellow bold]🤷 No workflows were provided in the deployment file![/yellow bold]")
        if requested_workflows:
            raise Exception(
                f"The following workflows were requested: {requested_workflows}, "
                f"but no workflows are defined in the deployment file."
            )

        raise typer.Exit()


def _create_workflows(workflows: WorkflowList, api_client: ApiClient) -> Dict[str, int]:
    deployment_data = {}
    for workflow in workflows:
        dbx_echo(f"Processing deployment for workflow: {workflow.name}")
        jobs_service = JobsService(api_client)
        matching_job = find_job_by_name(jobs_service, workflow.name)

        if not matching_job:
            job_id = _create_job(api_client, workflow)
        else:
            job_id = matching_job["job_id"]
            _update_job(jobs_service, job_id, workflow)

        deployment_data[workflow.name] = job_id
        workflow.job_id = job_id
    return deployment_data


def _create_job(api_client: ApiClient, workflow: AnyWorkflow) -> str:
    dbx_echo(f"Creating a new job with name {workflow.name}")
    workflow_definition = workflow.dict(exclude_none=True)
    try:
        jobs_api = JobsApi(api_client)
        job_id = jobs_api.create_job(workflow_definition)["job_id"]
    except HTTPError as e:
        dbx_echo(":boom: Failed to create job with definition:")
        dbx_echo(workflow_definition)
        raise e
    return job_id


def _update_job(jobs_service: JobsService, job_id: str, workflow: AnyWorkflow) -> str:
    dbx_echo(f"Updating existing job with id: {job_id} and name: {workflow.name}")
    workflow_definition = workflow.dict(exclude_none=True)
    try:
        jobs_service.reset_job(job_id, workflow_definition)
    except HTTPError as e:
        dbx_echo(":boom: Failed to update job with definition:")
        dbx_echo(workflow_definition)
        raise e

    return job_id
