import json
from pathlib import Path
from typing import List
from typing import Optional

import mlflow
import typer

from dbx.api.adjuster.adjuster import Adjuster, AdditionalLibrariesProvider
from dbx.api.config_reader import ConfigReader
from dbx.api.dependency.core_package import CorePackageManager
from dbx.api.dependency.requirements import RequirementsFileProcessor
from dbx.api.deployment import WorkflowDeploymentManager
from dbx.api.storage.io import StorageIO
from dbx.models.workflow.common.workflow_types import WorkflowType
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

    core_package = CorePackageManager(build_config=config.build).core_package
    libraries_from_requirements = RequirementsFileProcessor(requirements_file).libraries if requirements_file else []

    _assets_only = assets_only if assets_only else files_only

    if _assets_only:
        any_pipelines = [w for w in deployable_workflows if w.workflow_type == WorkflowType.pipeline]
        if any_pipelines:
            raise Exception(
                f"Assets-only deployment mode is not supported for DLT pipelines: {[p.name for p in any_pipelines]}"
            )

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

        pipelines = [p for p in deployable_workflows if p.workflow_type == WorkflowType.pipeline]
        workflows = [w for w in deployable_workflows if w.workflow_type != WorkflowType.pipeline]

        if pipelines:
            dbx_echo("Found DLT pipelines definition, applying them first for proper reference resolution")
            for elements in [pipelines, workflows]:
                adjuster.traverse(elements)
                wf_manager = WorkflowDeploymentManager(api_client, elements)
                wf_manager.apply()
        else:
            adjuster.traverse(deployable_workflows)
            if not _assets_only:
                wf_manager = WorkflowDeploymentManager(api_client, deployable_workflows)
                wf_manager.apply()

        deployment_tags = {
            "dbx_action_type": "deploy",
            "dbx_environment": environment_name,
            "dbx_status": "SUCCESS",
            "dbx_branch_name": branch_name,
        }

        deployment_tags.update(additional_tags)
        if _assets_only:
            deployment_tags["dbx_deploy_type"] = "files_only"

        environment_spec = environment_info.to_spec()

        StorageIO.save(environment_spec, "deployment-result.json")

        mlflow.set_tags(deployment_tags)
        dbx_echo(f":sparkles: Deployment for environment {environment_name} finished successfully")

        if write_specs_to_file:
            dbx_echo("Writing final job specifications into file")
            specs_file = Path(write_specs_to_file)

            if specs_file.exists():
                specs_file.unlink()

            specs_file.write_text(json.dumps(environment_spec, indent=4), encoding="utf-8")
