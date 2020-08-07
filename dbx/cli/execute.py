import copy
import os
from pathlib import Path

import click
import time
from databricks_cli.clusters.api import ClusterService
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.utils import CONTEXT_SETTINGS
from retry import retry
from setuptools import sandbox

from dbx.cli.utils import provide_lockfile_controller, LockFileController, read_json, INFO_FILE_NAME

"""
Logic behind this functionality:
1. Build a .whl file locally
2. Upload .whl to dbfs
3. Trigger %pip install --upgrade {path-to-file} magic
4. Execute entrypoint.py code
"""


@click.command(context_settings=CONTEXT_SETTINGS, short_help="Executes job on the cluster")
@click.option("--job-name", required=True, type=str, help="name of a job to be executed")
@debug_option
@profile_option
@provide_api_client
@provide_lockfile_controller
def execute(api_client: ApiClient, lockfile_controller: LockFileController, job_name):
    """
    Executes given job on the cluster by it's name. Please ensure that:
    1. Job exists under `jobs` directory
    2. There is an `entrypoint.py` file
    """
    click.echo("[dbx] Starting execution for job %s" % job_name)
    if not lockfile_controller.get_dev_cluster_id():
        msg = """Couldn't start execution as there is no cluster_id provided in .dbx.lock.json.
                Please ensure that the dev cluster was created"""
        raise click.exceptions.UsageError(msg)

    project_name = read_json(INFO_FILE_NAME)["project_name"]
    v1_client = get_v1_client(api_client)

    click.echo("[dbx] Building whl file")
    whl_file = build_project_whl()
    dbfs_package_location = upload_whl(api_client, project_name, lockfile_controller.get_uuid(), whl_file)

    cluster_id = lockfile_controller.get_dev_cluster_id()
    cluster_service = ClusterService(api_client)
    awake_cluster(cluster_service, cluster_id)

    context_id = get_context_id(v1_client, lockfile_controller)

    verbose_callback = lambda command: execute_command(v1_client, cluster_id, context_id, command)
    silent_callback = lambda command: execute_command(v1_client, cluster_id, context_id, command, verbose=False)

    upgrade_package(dbfs_package_location, silent_callback)
    execute_entrypoint(project_name, job_name, verbose_callback)


def build_project_whl() -> str:
    sandbox.run_setup('setup.py', ['-q', 'clean', 'bdist_wheel'])
    whl_file = os.listdir("dist")[0]
    return whl_file


def upload_file(api_client, src, dest):
    dbfs_api = DbfsApi(api_client)
    dbfs_api.put_file(src, DbfsPath(dest), True)


def upload_whl(api_client, project_name, uuid, whl_file):
    local_wheel_path = os.path.join(os.getcwd(), "dist", whl_file)
    remote_wheel_path = "dbfs:/tmp/dbx/%s/%s/%s" % (project_name, uuid, whl_file)
    upload_file(api_client, local_wheel_path, remote_wheel_path)
    return remote_wheel_path


def upgrade_package(dbfs_package_location: str, execution_callback):
    print("Installing newest package version")
    localized_name = dbfs_package_location.replace("dbfs:/", "/dbfs/")
    command = "%pip install --upgrade " + localized_name
    execution_callback(command)


def get_v1_client(api_client: ApiClient):
    v1_client = copy.deepcopy(api_client)
    v1_client.url = v1_client.url.replace('/api/2.0', '/api/1.2')
    return v1_client


def get_context_id(v1_client: ApiClient, lockfile_controller: LockFileController):
    context_id = lockfile_controller.get_execution_context_id()
    cluster_id = lockfile_controller.get_dev_cluster_id()
    if context_id:
        # context exists in lockfile and on DB
        if context_exists(v1_client, cluster_id, context_id):
            return context_id
    else:
        context_id = create_context(v1_client, cluster_id)
        lockfile_controller.update({"execution_context_id": context_id})
        return context_id


def context_exists(client, cluster_id, context_id):
    try:
        client.perform_query(method='GET', path='/contexts/status',
                             data={'clusterId': cluster_id, 'contextId': context_id})
        return True
    except:
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
        click.echo("[dbx] Dev cluster is running")
    if cluster_info["state"] in ["TERMINATED", "TERMINATING"]:
        click.echo("Dev cluster is terminated, starting it")
        cluster_service.start_cluster(cluster_id)
        time.sleep(5)
        awake_cluster(cluster_service, cluster_id)
    elif cluster_info["state"] == "ERROR":
        raise RuntimeError("Cluster is misconfigured and cannot be started, please check cluster settings at first")
    elif cluster_info["state"] in ["PENDING", "RESTARTING"]:
        click.echo("Cluster is getting prepared, current state: %s" % cluster_info["state"])
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
        click.echo("Command cancelled")
    else:
        final_result = execution_result["results"]["resultType"]
        if final_result == 'error':
            raise RuntimeError(execution_result["results"]["cause"])
        else:
            if verbose:
                click.echo(execution_result["results"]["data"])


def execute_entrypoint(project_name, job_name, execution_callback):
    entrypoint_file = "%s/jobs/%s/entrypoint.py" % (project_name, job_name)
    content = Path(entrypoint_file).read_text()
    execution_callback(content)
