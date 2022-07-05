import time
from pathlib import Path, PosixPath
from typing import Optional

import click
from databricks_cli.clusters.api import ClusterService
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.api.config_reader import ConfigReader
from dbx.api.driver_client import RichExecutionContextClient
from dbx.server.client import FileServerClient
from dbx.utils import dbx_echo
from dbx.utils.common import (
    prepare_environment,
    get_package_file,
    _preprocess_cluster_args,
    handle_package,
)
from dbx.utils.options import environment_option


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
    "--deployment-file",
    required=False,
    type=click.Path(path_type=Path),
    help="Path to deployment file.",
)
@environment_option
@debug_option
def execute(
    environment: str,
    cluster_id: str,
    cluster_name: str,
    job: str,
    deployment_file: Optional[Path],
):
    api_client = prepare_environment(environment)

    cluster_id = _preprocess_cluster_args(api_client, cluster_name, cluster_id)

    dbx_echo(f"Executing job: {job} in environment {environment} on cluster {cluster_name} (id: {cluster_id})")

    handle_package()
    package_file = get_package_file()

    config_reader = ConfigReader(deployment_file)
    deployment = config_reader.get_environment(environment)
    _verify_deployment(deployment, environment, deployment_file)

    found_jobs = [j for j in deployment["jobs"] if j["name"] == job]

    if not found_jobs:
        raise RuntimeError(f"Job {job} was not found in environment jobs, please check the deployment file")

    job_payload = found_jobs[0]

    if "tasks" in job_payload:
        raise NotImplementedError("Running jobs with tasks is currently not supported in dbx.")

    entrypoint_file = job_payload.get("spark_python_task").get("python_file").replace("file://", "")

    if not entrypoint_file:
        raise FileNotFoundError(
            f"No entrypoint file provided in job {job}. " f"Please add one under spark_python_task.python_file section"
        )

    cluster_service = ClusterService(api_client)
    dbx_echo("Preparing interactive cluster to accept jobs")
    awake_cluster(cluster_service, cluster_id)

    ec_client = RichExecutionContextClient(api_client, cluster_id)
    fs_client = FileServerClient(workspace_id=ec_client.get_workspace_id(), cluster_id=cluster_id)

    server_info = fs_client.get_server_info()
    driver_path_prefix = PosixPath(server_info["root_path"]) / ec_client.get_context_id()

    files_to_upload = [package_file]

    task_parameters = job_payload.get("spark_python_task").get("parameters", [])

    for idx, parameter in enumerate(task_parameters):
        if parameter.startswith("file:fuse://"):
            formatted_local_path = Path(parameter.replace("file:fuse://", ""))
            files_to_upload.append(formatted_local_path)
            task_parameters[idx] = driver_path_prefix / formatted_local_path.name

    fs_client.upload_files(files=files_to_upload, context_id=ec_client.get_context_id())

    if package_file:
        dbx_echo("Installing core package requirements")
        ec_client.install_package(driver_path_prefix / package_file.name)
        dbx_echo("Installing core package requirements - done")

    ec_client.setup_arguments(task_parameters)

    dbx_echo("Starting entrypoint file execution")
    ec_client.execute_file(Path(entrypoint_file))


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
