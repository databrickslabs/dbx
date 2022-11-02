from pathlib import Path
from typing import Optional

import typer

from dbx.api.cluster import ClusterController
from dbx.api.config_reader import ConfigReader
from dbx.api.configure import ProjectConfigurationManager
from dbx.api.context import RichExecutionContextClient
from dbx.api.dependency.core_package import CorePackageManager
from dbx.api.execute import ExecutionController
from dbx.models.cli.execute import ExecuteParametersPayload
from dbx.models.workflow.common.workflow_types import WorkflowType
from dbx.options import (
    DEPLOYMENT_FILE_OPTION,
    ENVIRONMENT_OPTION,
    REQUIREMENTS_FILE_OPTION,
    NO_REBUILD_OPTION,
    NO_PACKAGE_OPTION,
    JINJA_VARIABLES_FILE_OPTION,
    DEBUG_OPTION,
    WORKFLOW_ARGUMENT,
    EXECUTE_PARAMETERS_OPTION,
)
from dbx.types import ExecuteTask
from dbx.utils import dbx_echo
from dbx.utils.common import prepare_environment


def execute(
    workflow_name: str = WORKFLOW_ARGUMENT,
    environment: str = ENVIRONMENT_OPTION,
    cluster_id: Optional[str] = typer.Option(
        None, "--cluster-id", help="Cluster ID. Cannot be provided together with `--cluster-name`"
    ),
    cluster_name: Optional[str] = typer.Option(
        None, "--cluster-name", help="Cluster name. Cannot be provided together with `--cluster-id`"
    ),
    job_name: str = typer.Option(
        None, "--job", help="This option is deprecated. Please use `workflow-name` as argument instead"
    ),
    task_name: Optional[str] = typer.Option(
        None,
        "--task",
        help="""Task name (`task_key` field) inside the workflow to be executed.
        Required if the workflow is a multitask job with more than one task""",
    ),
    deployment_file: Path = DEPLOYMENT_FILE_OPTION,
    requirements_file: Optional[Path] = REQUIREMENTS_FILE_OPTION,
    no_rebuild: bool = NO_REBUILD_OPTION,
    no_package: bool = NO_PACKAGE_OPTION,
    upload_via_context: bool = typer.Option(
        False,
        "--upload-via-context",
        is_flag=True,
        help="Upload files via execution context",
    ),
    pip_install_extras: Optional[str] = typer.Option(
        None,
        "--pip-install-extras",
        help="""If provided, adds [<provided>] to the pip install command during
        package installation in the Databricks context.


        Useful when core package has extras section and installation of these extras is required.""",
    ),
    jinja_variables_file: Optional[Path] = JINJA_VARIABLES_FILE_OPTION,
    parameters: Optional[str] = EXECUTE_PARAMETERS_OPTION,
    debug: Optional[bool] = DEBUG_OPTION,  # noqa
):
    api_client = prepare_environment(environment)
    cluster_controller = ClusterController(api_client, cluster_name=cluster_name, cluster_id=cluster_id)

    workflow_name = workflow_name if workflow_name else job_name

    if not workflow_name:
        raise Exception("Please provide workflow name as an argument")

    dbx_echo(
        f"Executing workflow: {workflow_name} in environment {environment} "
        f"on cluster {cluster_name} (id: {cluster_id})"
    )

    config_reader = ConfigReader(deployment_file, jinja_variables_file)

    config = config_reader.get_config()
    environment_config = config.get_environment(environment, raise_if_not_found=True)

    if no_rebuild:
        dbx_echo(
            """[yellow bold]
        Legacy [code]--no-rebuild[/code] flag has been used.
        Please specify build logic in the build section of the deployment file instead.[/yellow bold]"""
        )
        config.build.no_build = True

    workflow = environment_config.payload.get_workflow(workflow_name)

    if workflow.workflow_type == WorkflowType.pipeline:
        raise Exception("DLT pipelines are not supported in the execute mode.")

    if not task_name and workflow.workflow_type == WorkflowType.job_v2d1:
        if len(workflow.task_names) == 1:
            dbx_echo("Task key wasn't provided, automatically picking it since there is only one task in the workflow")
            task_name = workflow.task_names[0]
        else:
            raise ValueError("Task key is not provided and there is more than one task in the workflow.")

    task: ExecuteTask = workflow.get_task(task_name) if task_name else workflow

    task.check_if_supported_in_execute()
    core_package = CorePackageManager(config.build).core_package if not no_package else None

    if parameters:
        task.override_execute_parameters(ExecuteParametersPayload.from_json(parameters))

    cluster_controller.awake_cluster()

    context_client = RichExecutionContextClient(api_client, cluster_controller.cluster_id)

    upload_via_context = (
        upload_via_context
        if upload_via_context
        else ProjectConfigurationManager().get_context_based_upload_for_execute()
    )

    execution_controller = ExecutionController(
        client=context_client,
        no_package=no_package,
        core_package=core_package,
        requirements_file=requirements_file,
        task=task,
        upload_via_context=upload_via_context,
        pip_install_extras=pip_install_extras,
    )
    execution_controller.run()
