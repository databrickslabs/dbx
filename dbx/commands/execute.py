import time
from pathlib import Path
from typing import Optional

import click
from databricks_cli.clusters.api import ClusterService
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.api.config_reader import ConfigReader
from dbx.api.context import RichExecutionContextClient
from dbx.api.execute import ExecutionController
from dbx.utils import dbx_echo
from dbx.utils.common import (
    prepare_environment,
    handle_package,
    _preprocess_cluster_args,
)
from dbx.utils.options import environment_option, deployment_file_option


@click.command(
    context_settings=CONTEXT_SETTINGS,
    short_help="Executes given job on the interactive cluster.",
    help="""
    Executes given job on the interactive cluster.

    This command is very suitable to interactively execute your code on the interactive clusters.

    .. warning::

        There are some limitations for :code:`dbx execute`:

        * Only clusters which support :code:`%pip` magic can work with execute.
        * Currently, only Python-based execution is supported.

    The following set of actions will be done during execution:

    1. If interactive cluster is stopped, it will be automatically started
    2. Package will be rebuilt from the source (can be disabled via :option:`--no-rebuild`)
    3. Job configuration will be taken from deployment file for given environment
    4. All referenced will be uploaded to the MLflow experiment
    5. | Code will be executed in a separate context. Other users can work with the same package
       | on the same cluster without any limitations or overlapping.
    6. Execution results will be printed out in the shell. If result was an error, command will have error exit code.

    """,
)
@click.option("--cluster-id", required=False, type=str, help="Cluster ID.")
@click.option("--cluster-name", required=False, type=str, help="Cluster name.")
@click.option("--job", required=True, type=str, help="Job name to be executed")
@click.option(
    "--task",
    required=False,
    type=str,
    help="Task name (task_key field) inside the job to be executed. Required if the --job is a multitask job.",
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
    "--upload-via-context",
    is_flag=True,
    default=False,
    help="Upload files via execution context",
)
@environment_option
@debug_option
@deployment_file_option
def execute(
    environment: str,
    cluster_id: str,
    cluster_name: str,
    job: str,
    task: Optional[str],
    deployment_file: Optional[Path],
    requirements_file: Path,
    no_package: bool,
    no_rebuild: bool,
    upload_via_context: bool,
):
    api_client = prepare_environment(environment)

    cluster_id = _preprocess_cluster_args(api_client, cluster_name, cluster_id)

    dbx_echo(f"Executing job: {job} in environment {environment} on cluster {cluster_name} (id: {cluster_id})")

    handle_package(no_rebuild)

    config_reader = ConfigReader(deployment_file)

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

    entrypoint_file = _payload.get("spark_python_task", {}).get("python_file")

    if not entrypoint_file:
        raise Exception(
            f"No entrypoint file provided in job {job}, or the job is not a spark_python_task. \n"
            "Currently, only spark_python_task jobs and tasks are supported for dbx execute."
        )

    entrypoint_file_path = Path(entrypoint_file.replace("file://", ""))

    cluster_service = ClusterService(api_client)

    dbx_echo("Preparing interactive cluster to accept jobs")
    awake_cluster(cluster_service, cluster_id)

    context_client = RichExecutionContextClient(api_client, cluster_id)
    task_parameters = _payload.get("spark_python_task").get("parameters", [])

    controller_instance = ExecutionController(
        client=context_client,
        no_package=no_package,
        requirements_file=requirements_file,
        entrypoint_file=entrypoint_file_path,
        task_parameters=task_parameters,
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
