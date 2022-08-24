from pathlib import Path
from typing import Optional

import typer

from dbx.api.cluster import ClusterController
from dbx.api.config_reader import ConfigReader
from dbx.api.context import RichExecutionContextClient
from dbx.api.execute import ExecutionController
from dbx.models.deployment import EnvironmentDeploymentInfo
from dbx.models.parameters.execute import ExecuteWorkloadParamInfo
from dbx.models.task import Task, TaskType
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
from dbx.utils import dbx_echo
from dbx.utils.common import prepare_environment
from dbx.api.build import prepare_build


def execute(
    workflow: str = WORKFLOW_ARGUMENT,
    environment: str = ENVIRONMENT_OPTION,
    cluster_id: Optional[str] = typer.Option(
        None, "--cluster-id", help="Cluster ID. Cannot be provided together with `--cluster-name`"
    ),
    cluster_name: Optional[str] = typer.Option(
        None, "--cluster-name", help="Cluster name. Cannot be provided together with `--cluster-id`"
    ),
    job: str = typer.Option(
        None, "--job", help="This option is deprecated. Please use `workflow-name` as argument instead"
    ),
    task: Optional[str] = typer.Option(
        None,
        "--task",
        help="""Task name (`task_key` field) inside the workflow to be executed.
        Required if the workflow is a multitask job""",
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
    jinja_variables_file: Optional[Path] = JINJA_VARIABLES_FILE_OPTION,
    parameters: Optional[str] = EXECUTE_PARAMETERS_OPTION,
    debug: Optional[bool] = DEBUG_OPTION,  # noqa
):
    api_client = prepare_environment(environment)
    controller = ClusterController(api_client)
    cluster_id = controller.preprocess_cluster_args(cluster_name, cluster_id)

    _job = workflow if workflow else job

    if not _job:
        raise Exception("Please provide workflow name as an argument")

    dbx_echo(f"Executing job: {_job} in environment {environment} on cluster {cluster_name} (id: {cluster_id})")

    config_reader = ConfigReader(deployment_file, jinja_variables_file)

    config = config_reader.get_config()
    deployment = config.get_environment(environment)

    if no_rebuild:
        dbx_echo(
            """[yellow bold]
        Legacy [code]--no-rebuild[/code] flag has been used.
        Please specify build logic in the build section of the deployment file instead.[/yellow bold]"""
        )
        config.build.no_build = True

    prepare_build(config.build)

    _verify_deployment(deployment, deployment_file)

    found_jobs = [j for j in deployment.payload.workflows if j["name"] == _job]

    if not found_jobs:
        raise RuntimeError(f"Job {_job} was not found in environment jobs, please check the deployment file")

    job_payload = found_jobs[0]

    if task:
        _tasks = job_payload.get("tasks", [])
        found_tasks = [t for t in _tasks if t.get("task_key") == task]

        if not found_tasks:
            raise Exception(f"Task {task} not found in the definition of job {_job}")

        if len(found_tasks) > 1:
            raise Exception(f"Task keys are not unique, more then one task found for job {_job} with task name {task}")

        _task = found_tasks[0]

        _payload = _task
    else:
        if "tasks" in job_payload:
            raise Exception(
                "You're trying to execute a multitask job without passing the task name. "
                "Please provide the task name via --task parameter"
            )
        _payload = job_payload

    task = Task(**_payload)

    if parameters:
        override_parameters(parameters, task)

    dbx_echo("Preparing interactive cluster to accept jobs")
    controller.awake_cluster(cluster_id)

    context_client = RichExecutionContextClient(api_client, cluster_id)

    controller_instance = ExecutionController(
        client=context_client,
        no_package=no_package,
        requirements_file=requirements_file,
        task=task,
        upload_via_context=upload_via_context,
    )
    controller_instance.run()


def _verify_deployment(deployment: EnvironmentDeploymentInfo, deployment_file):
    if not deployment:
        raise NameError(
            f"Environment {deployment.name} is not provided in deployment file {deployment_file}"
            + " please add this environment first"
        )
    env_jobs = deployment.payload.workflows
    if not env_jobs:
        raise RuntimeError(f"No jobs section found in environment {deployment.name}, please check the deployment file")


def override_parameters(raw_params_info: str, task: Task):
    param_info = ExecuteWorkloadParamInfo.from_string(raw_params_info)
    if param_info.named_parameters is not None and task.task_type != TaskType.python_wheel_task:
        raise Exception(f"named parameters are only supported if task type is {TaskType.python_wheel_task.value}")

    if param_info.named_parameters:
        dbx_echo(":twisted_rightwards_arrows:Overriding named_parameters section for the task")
        task.python_wheel_task.named_parameters = param_info.named_parameters
        task.python_wheel_task.parameters = []
        dbx_echo(":white_check_mark:Overriding named_parameters section for the task")

    if param_info.parameters:
        dbx_echo(":twisted_rightwards_arrows:Overriding parameters section for the task")

        if task.task_type == TaskType.python_wheel_task:
            task.python_wheel_task.parameters = param_info.parameters
            task.python_wheel_task.named_parameters = []
        elif task.task_type == TaskType.spark_python_task:
            task.spark_python_task.parameters = param_info.parameters

        dbx_echo(":white_check_mark:Overriding parameters section for the task")
