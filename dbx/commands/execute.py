import time
from pathlib import Path
from typing import Optional

import typer
from databricks_cli.clusters.api import ClusterService

from dbx.api.config_reader import ConfigReader
from dbx.api.context import RichExecutionContextClient
from dbx.api.execute import ExecutionController
from dbx.models.task import Task
from dbx.options import (
    DEPLOYMENT_FILE_OPTION,
    ENVIRONMENT_OPTION,
    REQUIREMENTS_FILE_OPTION,
    NO_REBUILD_OPTION,
    NO_PACKAGE_OPTION,
    JINJA_VARIABLES_FILE_OPTION, DEBUG_OPTION,
)
from dbx.utils import dbx_echo
from dbx.utils.common import (
    prepare_environment,
    handle_package,
    _preprocess_cluster_args,
)


def execute(
    environment: str = ENVIRONMENT_OPTION,
    cluster_id: Optional[str] = typer.Option(None, "--cluster-id", help="Cluster ID."),
    cluster_name: Optional[str] = typer.Option(None, "--cluster-name", help="Cluster name."),
    job: str = typer.Option(..., "--job", help="[red]This option is deprecated[/red]"),
    task: Optional[str] = typer.Option(
        None,
        "--task",
        help="""Task name (task_key field) inside the job to be executed.
             [red bold]Required if the --job is a multitask job[/red bold].""",
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
    debug: Optional[bool] = DEBUG_OPTION
):
    api_client = prepare_environment(environment)

    cluster_id = _preprocess_cluster_args(api_client, cluster_name, cluster_id)

    dbx_echo(f"Executing job: {job} in environment {environment} on cluster {cluster_name} (id: {cluster_id})")

    handle_package(no_rebuild)

    config_reader = ConfigReader(deployment_file, jinja_variables_file)

    deployment = config_reader.get_environment(environment)

    _verify_deployment(deployment, environment, deployment_file)

    found_jobs = [j for j in deployment["jobs"] if j["name"] == job]

    if not found_jobs:
        raise RuntimeError(f"Job {job} was not found in environment jobs, please check the deployment file")

    job_payload = found_jobs[0]

    if task:
        _tasks = job_payload.get("tasks", [])
        found_tasks = [t for t in _tasks if t.get("task_key") == task]

        if not found_tasks:
            raise Exception(f"Task {task} not found in the definition of job {job}")

        if len(found_tasks) > 1:
            raise Exception(f"Task keys are not unique, more then one task found for job {job} with task name {task}")

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

    cluster_service = ClusterService(api_client)

    dbx_echo("Preparing interactive cluster to accept jobs")
    awake_cluster(cluster_service, cluster_id)

    context_client = RichExecutionContextClient(api_client, cluster_id)

    controller_instance = ExecutionController(
        client=context_client,
        no_package=no_package,
        requirements_file=requirements_file,
        task=task,
        upload_via_context=upload_via_context,
    )
    controller_instance.run()


def _verify_deployment(deployment, environment, deployment_file):
    if not deployment:
        raise NameError(
            f"Environment {environment} is not provided in deployment file {deployment_file}"
            + " please add this environment first"
        )
    env_jobs = deployment.get("jobs")
    if not env_jobs:
        raise RuntimeError(f"No jobs section found in environment {environment}, please check the deployment file")


def awake_cluster(cluster_service: ClusterService, cluster_id):
    cluster_info = cluster_service.get_cluster(cluster_id)
    if cluster_info["state"] in ["RUNNING", "RESIZING"]:
        dbx_echo("Cluster is ready")
    if cluster_info["state"] in ["TERMINATED", "TERMINATING"]:
        dbx_echo("Dev cluster is terminated, starting it")
        cluster_service.start_cluster(cluster_id)
        time.sleep(5)
        awake_cluster(cluster_service, cluster_id)
    elif cluster_info["state"] == "ERROR":
        raise RuntimeError("Cluster is mis-configured and cannot be started, please check cluster settings at first")
    elif cluster_info["state"] in ["PENDING", "RESTARTING"]:
        dbx_echo(f'Cluster is getting prepared, current state: {cluster_info["state"]}')
        time.sleep(5)
        awake_cluster(cluster_service, cluster_id)
