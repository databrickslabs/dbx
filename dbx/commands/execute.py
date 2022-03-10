import pathlib
import time
from typing import Any, List, Optional

import click
import mlflow
from databricks_cli.clusters.api import ClusterService
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.commands.deploy import finalize_deployment_file_path
from dbx.utils.adjuster import walk_content, adjust_path
from dbx.utils.common import (
    dbx_echo,
    prepare_environment,
    FileUploader,
    ContextLockFile,
    ApiV1Client,
    environment_option,
    get_deployment_config,
    handle_package,
    get_package_file,
    _preprocess_cluster_args,
)


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
    type=str,
    help="Path to deployment file in one of these formats: [json, yaml]",
)
@click.option("--requirements-file", required=False, type=str, default="requirements.txt")
@click.option("--no-rebuild", is_flag=True, help="Disable package rebuild")
@click.option(
    "--no-package",
    is_flag=True,
    help="Do not add package reference into the job description",
)
@environment_option
@debug_option
def execute(
    environment: str,
    cluster_id: str,
    cluster_name: str,
    job: str,
    deployment_file: Optional[str],
    requirements_file: str,
    no_package: bool,
    no_rebuild: bool,
):
    api_client = prepare_environment(environment)

    cluster_id = _preprocess_cluster_args(api_client, cluster_name, cluster_id)

    dbx_echo(f"Executing job: {job} in environment {environment} on cluster {cluster_name} (id: {cluster_id})")

    handle_package(no_rebuild)

    deployment_file = finalize_deployment_file_path(deployment_file)

    deployment = get_deployment_config(deployment_file).get_environment(environment)
    is_strict = deployment.get("strict_path_adjustment_policy", False)

    _verify_deployment(deployment, environment, deployment_file)

    found_jobs = [j for j in deployment["jobs"] if j["name"] == job]

    if not found_jobs:
        raise RuntimeError(f"Job {job} was not found in environment jobs, please check the deployment file")

    job_payload = found_jobs[0]

    entrypoint_file = job_payload.get("spark_python_task").get("python_file")

    if not entrypoint_file:
        raise FileNotFoundError(
            f"No entrypoint file provided in job {job}. " f"Please add one under spark_python_task.python_file section"
        )
    entrypoint_file = entrypoint_file if not is_strict else entrypoint_file.replace("file://", "")

    cluster_service = ClusterService(api_client)

    dbx_echo("Preparing interactive cluster to accept jobs")
    awake_cluster(cluster_service, cluster_id)

    v1_client = ApiV1Client(api_client)
    context_id = get_context_id(v1_client, cluster_id, "python")

    with mlflow.start_run() as execution_run:

        artifact_base_uri = execution_run.info.artifact_uri
        file_uploader = FileUploader(artifact_base_uri, is_strict)

        requirements_fp = pathlib.Path(requirements_file)
        if requirements_fp.exists():

            localized_requirements_path = file_uploader.upload_and_provide_path(requirements_fp, as_fuse=True)

            installation_command = f"%pip install -U -r {localized_requirements_path}"

            dbx_echo("Installing provided requirements")
            execute_command(v1_client, cluster_id, context_id, installation_command, verbose=False)
            dbx_echo("Provided requirements installed")
        else:
            dbx_echo(
                f"Requirements file {requirements_fp} is not provided"
                + ", following the execution without any additional packages"
            )

        if not no_package:
            package_file = get_package_file()

            if not package_file:
                raise FileNotFoundError("Project package was not found. Please check that /dist directory exists.")

            localized_package_path = file_uploader.upload_and_provide_path(package_file, as_fuse=True)

            dbx_echo("Installing package")
            installation_command = f"%pip install --force-reinstall {localized_package_path}"
            execute_command(v1_client, cluster_id, context_id, installation_command, verbose=False)
            dbx_echo("Package installation finished")
        else:
            dbx_echo("Package was disabled via --no-package, only the code from entrypoint will be used")

        tags = {"dbx_action_type": "execute", "dbx_environment": environment}

        mlflow.set_tags(tags)

        dbx_echo("Processing parameters")
        task_props: List[Any] = job_payload.get("spark_python_task").get("parameters", [])

        if task_props:

            def adjustment_callback(p: Any):
                return adjust_path(p, file_uploader)

            walk_content(adjustment_callback, task_props)

        task_props = ["python"] + task_props

        parameters_command = f"""
        import sys
        sys.argv = {task_props}
        """

        execute_command(v1_client, cluster_id, context_id, parameters_command, verbose=False)

        dbx_echo("Processing parameters - done")

        dbx_echo("Starting entrypoint file execution")

        execute_command(v1_client, cluster_id, context_id, pathlib.Path(entrypoint_file).read_text(encoding="utf-8"))
        dbx_echo("Command execution finished")


def _verify_deployment(deployment, environment, deployment_file):
    if not deployment:
        raise NameError(
            f"Environment {environment} is not provided in deployment file {deployment_file}"
            + " please add this environment first"
        )
    env_jobs = deployment.get("jobs")
    if not env_jobs:
        raise RuntimeError(f"No jobs section found in environment {environment}, please check the deployment file")


def wait_for_command_execution(v1_client: ApiV1Client, cluster_id: str, context_id: str, command_id: str):
    finished = False
    payload = {
        "clusterId": cluster_id,
        "contextId": context_id,
        "commandId": command_id,
    }
    while not finished:
        try:
            result = v1_client.get_command_status(payload)
            status = result.get("status")
            if status in ["Finished", "Cancelled", "Error"]:
                return result
            else:
                time.sleep(5)
        except KeyboardInterrupt:
            v1_client.cancel_command(payload)


def execute_command(v1_client: ApiV1Client, cluster_id: str, context_id: str, command: str, verbose=True):
    payload = {
        "language": "python",
        "clusterId": cluster_id,
        "contextId": context_id,
        "command": command,
    }
    command_execution_data = v1_client.execute_command(payload)
    command_id = command_execution_data["id"]
    execution_result = wait_for_command_execution(v1_client, cluster_id, context_id, command_id)
    if execution_result["status"] == "Cancelled":
        dbx_echo("Command cancelled")
    else:
        final_result = execution_result["results"]["resultType"]
        if final_result == "error":
            dbx_echo("Execution failed, please follow the given error")
            raise RuntimeError(
                f"Command execution failed. " f'Cluster error cause: {execution_result["results"]["cause"]}'
            )

        if verbose:
            dbx_echo("Command successfully executed")
            print(execution_result["results"]["data"])

        return execution_result["results"]["data"]


def _is_context_available(v1_client: ApiV1Client, cluster_id: str, context_id: str):
    if not context_id:
        return False
    else:
        payload = {"clusterId": cluster_id, "contextId": context_id}
        resp = v1_client.get_context_status(payload)
        if not resp:
            return False
        elif resp.get("status"):
            return resp["status"] == "Running"


def get_context_id(v1_client: ApiV1Client, cluster_id: str, language: str):
    dbx_echo("Preparing execution context")
    lock_context_id = ContextLockFile.get_context()

    if _is_context_available(v1_client, cluster_id, lock_context_id):
        dbx_echo("Existing context is active, using it")
        return lock_context_id
    else:
        dbx_echo("Existing context is not active, creating a new one")
        context_id = create_context(v1_client, cluster_id, language)
        ContextLockFile.set_context(context_id)
        dbx_echo("New context prepared, ready to use it")
        return context_id


def create_context(v1_client: ApiV1Client, cluster_id: str, language: str):
    payload = {"language": language, "clusterId": cluster_id}
    response = v1_client.create_context(payload)
    return response["id"]


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
