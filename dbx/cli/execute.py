import copy
from pathlib import Path

import click
import mlflow
import time
from databricks_cli.clusters.api import ClusterService
from databricks_cli.configure.config import provide_api_client, debug_option
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.utils import CONTEXT_SETTINGS
from retry import retry

from dbx.cli.utils import LockFile, InfoFile, dbx_echo, setup_mlflow, custom_profile_option, extract_version, \
    build_project_whl, upload_whl

"""
Logic behind this functionality:
0. Start mlflow run
1. Build a .whl file locally
2. Upload .whl to mlfow
3. Trigger %pip install --upgrade {path-to-file} magic
4. Execute entrypoint.py code
"""


@click.command(context_settings=CONTEXT_SETTINGS, short_help="Executes job on the cluster")
@click.option("--job-name", required=True, type=str, help="name of a job to be executed")
@debug_option
@custom_profile_option
@provide_api_client
@setup_mlflow
def execute(api_client: ApiClient, job_name):
    """
    Executes given job on the cluster by it's name. Please ensure that:
    1. Job exists under `jobs` directory
    2. There is an `entrypoint.py` file
    """
    dbx_echo("Starting execution for job %s" % job_name)
    if not LockFile.get("dev_cluster_id"):
        msg = """Couldn't start execution as there is no cluster_id provided in .dbx.lock.json.
                Please ensure that the dev cluster was created"""
        raise click.exceptions.UsageError(msg)

    project_name = InfoFile.get("project_name")
    v1_client = get_v1_client(api_client)

    with mlflow.start_run():
        dbx_echo("Building whl file")
        whl_file = build_project_whl()
        upload_whl(whl_file)
        package_version = extract_version(whl_file)
        dbfs_package_location = mlflow.get_artifact_uri(whl_file)

        cluster_id = LockFile.get("dev_cluster_id")
        cluster_service = ClusterService(api_client)

        dbx_echo("Preparing cluster to accept jobs")
        awake_cluster(cluster_service, cluster_id)

        context_id = get_context_id(v1_client)

        verbose_callback = lambda command: execute_command(v1_client, cluster_id, context_id, command)
        silent_callback = lambda command: execute_command(v1_client, cluster_id, context_id, command, verbose=False)

        upgrade_package(dbfs_package_location, silent_callback)
        execute_entrypoint(project_name, job_name, verbose_callback)

        tags = {
            "job_name": job_name,
            "dbx_uuid": LockFile.get("dbx_uuid"),
            "environment": "dev",
            "version": package_version,
            "action_type": "execute"
        }

        mlflow.set_tags(tags)


def upgrade_package(dbfs_package_location: str, execution_callback):
    dbx_echo("Upgrading package on the cluster")
    mlflow.get_artifact_uri()
    localized_name = dbfs_package_location.replace("dbfs:/", "/dbfs/")
    command = "%pip install --upgrade " + localized_name
    execution_callback(command)


def get_v1_client(api_client: ApiClient):
    v1_client = copy.deepcopy(api_client)
    v1_client.url = v1_client.url.replace('/api/2.0', '/api/1.2')
    return v1_client


def get_context_id(v1_client: ApiClient):
    dbx_echo("Preparing execution context")
    context_id = LockFile.get("execution_context_id")
    cluster_id = LockFile.get("dev_cluster_id")
    if context_id:
        # context exists in lockfile and on DB
        if context_exists(v1_client, cluster_id, context_id):
            return context_id
    else:
        context_id = create_context(v1_client, cluster_id)
        LockFile.update({"execution_context_id": context_id})
        return context_id


def context_exists(client, cluster_id, context_id):
    context_data = client.perform_query(method='GET', path='/contexts/status',
                                        data={'clusterId': cluster_id, 'contextId': context_id})
    if context_data["status"] == "Running":
        return True
    else:
        return False


@retry(tries=10, delay=1, backoff=5)
def create_context(v1_client, cluster_id):
    payload = v1_client.perform_query(method='POST',
                                      path='/contexts/create',
                                      data={'language': 'python', 'clusterId': cluster_id})
    return payload["id"]


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
        raise RuntimeError("Cluster is misconfigured and cannot be started, please check cluster settings at first")
    elif cluster_info["state"] in ["PENDING", "RESTARTING"]:
        dbx_echo("Cluster is getting prepared, current state: %s" % cluster_info["state"])
        time.sleep(10)
        awake_cluster(cluster_service, cluster_id)


def wait_for_command_execution(v1_client: ApiClient, cluster_id: str, context_id: str, command_id: str):
    finished = False
    payload = {'clusterId': cluster_id, 'contextId': context_id, 'commandId': command_id}
    while not finished:
        try:
            result = v1_client.perform_query(method='GET',
                                             path='/commands/status',
                                             data=payload)
            status = result.get("status")
            if status in ["Finished", "Cancelled", "Error"]:
                return result
            else:
                time.sleep(5)
        except KeyboardInterrupt:
            v1_client.perform_query(method='POST',
                                    path='/commands/cancel',
                                    data=payload)


def execute_command(v1_client: ApiClient, cluster_id: str, context_id: str, command: str, verbose=True):
    payload = {'language': 'python', 'clusterId': cluster_id, 'contextId': context_id, 'command': command}
    command_execution_data = v1_client.perform_query(method='POST',
                                                     path='/commands/execute',
                                                     data=payload)
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


def execute_entrypoint(project_name, job_name, execution_callback):
    dbx_echo("Launching execution from the entry point")
    entrypoint_file = "%s/jobs/%s/entrypoint.py" % (project_name, job_name)
    content = Path(entrypoint_file).read_text()
    execution_callback(content)
