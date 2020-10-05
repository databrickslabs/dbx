import pathlib

import click
import mlflow
import time
from databricks_cli.clusters.api import ClusterService
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.utils import CONTEXT_SETTINGS
from setuptools import sandbox
from dbx.utils.common import (
    dbx_echo, prepare_environment, upload_file, ContextLockFile, ApiV1Client,
    environment_option, DeploymentFile, DEFAULT_DEPLOYMENT_FILE_PATH
)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Executes given code on the existing cluster.")
@click.option("--cluster-id", required=False, type=str, help="Cluster ID.")
@click.option("--cluster-name", required=False, type=str, help="Cluster name.")
@click.option("--job", required=True, type=str, help="Job name to be executed")
@click.option("--deployment-file", required=False, type=str,
              help="Path to deployment file in json format", default=DEFAULT_DEPLOYMENT_FILE_PATH)
@click.option("--requirements-file", required=False, type=str, default="requirements.txt")
@click.option("--no-rebuild", is_flag=True, help="Disable job rebuild")
@environment_option
def execute(environment: str,
            cluster_id: str,
            cluster_name: str,
            job: str,
            deployment_file: str,
            requirements_file: str,
            no_rebuild: bool):
    api_client = prepare_environment(environment)

    cluster_id = _preprocess_cluster_args(api_client, cluster_name, cluster_id)

    dbx_echo("Executing job: %s with environment: %s on cluster: %s" % (job, environment, cluster_id))

    if not pathlib.Path("setup.py").exists():
        raise Exception("no setup.py provided in project directory. Please create one.")

    if no_rebuild:
        dbx_echo("No rebuild will be done, please ensure that the package distribution is in dist folder")
    else:
        sandbox.run_setup('setup.py', ['clean', 'bdist_wheel'])

    env_data = DeploymentFile(deployment_file).get_environment(environment)

    if not env_data:
        raise Exception(
            f"Environment {environment} is not provided in deployment file {deployment_file}" +
            " please add this environment first"
        )

    env_jobs = env_data.get("jobs")
    if not env_jobs:
        raise Exception(f"No jobs section found in environment {environment}, please check the deployment file")

    found_jobs = [j for j in env_data["jobs"] if j["name"] == job]

    if not found_jobs:
        raise Exception(f"Job {job} was not found in environment jobs, please check the deployment file")

    job_payload = found_jobs[0]

    entrypoint_file = job_payload.get("spark_python_task").get("python_file")

    if not entrypoint_file:
        raise Exception(f"No entrypoint file provided in job {job}. "
                        f"Please add one under spark_python_task.python_file section")

    cluster_service = ClusterService(api_client)

    dbx_echo("Preparing interactive cluster to accept jobs")
    awake_cluster(cluster_service, cluster_id)

    v1_client = ApiV1Client(api_client)
    context_id = get_context_id(v1_client, cluster_id, "python")

    with mlflow.start_run() as execution_run:

        artifact_base_uri = execution_run.info.artifact_uri
        localized_base_path = artifact_base_uri.replace("dbfs:/", "/dbfs/")

        requirements_fp = pathlib.Path(requirements_file)
        if requirements_fp.exists():
            upload_file(requirements_fp)
            localized_requirements_path = "%s/%s" % (localized_base_path, str(requirements_fp))
            installation_command = "%pip install -U -r {path}".format(path=localized_requirements_path)
            dbx_echo("Installing provided requirements")
            execute_command(v1_client, cluster_id, context_id, installation_command, verbose=False)
            dbx_echo("Provided requirements installed")

        project_package_path = list(pathlib.Path(".").rglob("dist/*.whl"))[0]
        upload_file(project_package_path)
        localized_package_path = "%s/%s" % (localized_base_path, str(project_package_path))
        installation_command = "%pip install --upgrade {path}".format(path=localized_package_path)
        execute_command(v1_client, cluster_id, context_id, installation_command, verbose=False)
        dbx_echo("Package installed")

        tags = {
            "dbx_action_type": "execute",
            "dbx_environment": environment
        }

        mlflow.set_tags(tags)

        dbx_echo("Starting entrypoint file execution")
        execute_command(v1_client, cluster_id, context_id, entrypoint_file)
        dbx_echo("Command execution finished")


def wait_for_command_execution(v1_client: ApiV1Client, cluster_id: str, context_id: str, command_id: str):
    finished = False
    payload = {'clusterId': cluster_id, 'contextId': context_id, 'commandId': command_id}
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
    payload = {'language': 'python', 'clusterId': cluster_id, 'contextId': context_id, 'command': command}
    command_execution_data = v1_client.execute_command(payload)
    command_id = command_execution_data['id']
    execution_result = wait_for_command_execution(v1_client, cluster_id, context_id, command_id)
    if execution_result["status"] == "Cancelled":
        dbx_echo("Command cancelled")
    else:
        final_result = execution_result["results"]["resultType"]
        if final_result == 'error':
            raise RuntimeError(execution_result["results"]["cause"])
        else:

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
        return context_id


def create_context(v1_client: ApiV1Client, cluster_id: str, language: str):
    payload = {'language': language, 'clusterId': cluster_id}
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
        raise Exception("Cluster is misconfigured and cannot be started, please check cluster settings at first")
    elif cluster_info["state"] in ["PENDING", "RESTARTING"]:
        dbx_echo("Cluster is getting prepared, current state: %s" % cluster_info["state"])
        time.sleep(10)
        awake_cluster(cluster_service, cluster_id)


def _preprocess_cluster_args(api_client: ApiClient, cluster_name, cluster_id) -> str:
    cluster_service = ClusterService(api_client)

    if not cluster_name and not cluster_id:
        raise Exception("Parameters cluster-name and cluster-id couldn't be empty at the same time.")

    if cluster_name:

        existing_clusters = cluster_service.list_clusters()["clusters"]
        matching_clusters = [c for c in existing_clusters if c["cluster_name"] == cluster_name]

        if not matching_clusters:
            raise Exception("No clusters with name %s found" % cluster_name)
        if len(matching_clusters) > 1:
            raise Exception("Found more then one cluster with name %s: %s" % (cluster_name, matching_clusters))

        cluster_id = matching_clusters[0]["cluster_id"]

    if cluster_id:
        if not cluster_service.get_cluster(cluster_id):
            raise Exception("Cluster with id %s not found" % cluster_id)

    return cluster_id
