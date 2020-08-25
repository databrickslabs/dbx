import copy
import pathlib

import click
import time
from databricks_cli.clusters.api import ClusterService
from databricks_cli.sdk.api_client import ApiClient
from retry import retry

from dbx.cli.utils import dbx_echo, _provide_environment, _adjust_context

SUFFIX_MAPPING = {
    ".py": "python",
    ".scala": "scala",
    ".R": "R",
    ".sql": "sql"
}


@click.command(context_settings=_adjust_context(),
               short_help="Executes given file on existing cluster")
@click.option("--environment", required=True, type=str, help="Environment name.")
@click.option("--cluster-id", required=False, type=str, help="Cluster ID.")
@click.option("--cluster-name", required=False, type=str, help="Cluster name.")
@click.option("--source-file", required=True, type=str, help="Path to the file with source code")
def execute(environment: str, cluster_id: str, cluster_name: str, source_file: str):
    dbx_echo("Launching job by given parameters")

    environment_data, api_client = _provide_environment(environment)

    cluster_id = _preprocess_cluster_args(api_client, cluster_name, cluster_id)

    source_file_obj = pathlib.Path(source_file)

    if not source_file_obj.exists():
        raise FileNotFoundError("Source file %s is non-existent" % source_file)

    source_file_content = source_file_obj.read_text()
    language = _define_language(source_file_obj)

    cluster_service = ClusterService(api_client)

    dbx_echo("Preparing cluster to accept jobs")
    awake_cluster(cluster_service, cluster_id)

    v1_client = get_v1_client(api_client)
    context_id = get_context_id(v1_client, cluster_id, language)

    execute_command(v1_client, cluster_id, context_id, source_file_content)


def wait_for_command_execution(v1_client: ApiClient, cluster_id: str, context_id: str, command_id: str):
    finished = False
    payload = {'clusterId': cluster_id, 'contextId': context_id, 'commandId': command_id}
    while not finished:
        try:
            result = v1_client.perform_query(method='GET', path='/commands/status', data=payload)
            status = result.get("status")
            if status in ["Finished", "Cancelled", "Error"]:
                return result
            else:
                time.sleep(5)
        except KeyboardInterrupt:
            v1_client.perform_query(method='POST', path='/commands/cancel', data=payload)


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


def get_v1_client(api_client: ApiClient):
    v1_client = copy.deepcopy(api_client)
    v1_client.url = v1_client.url.replace('/api/2.0', '/api/1.2')
    return v1_client


def get_context_id(v1_client: ApiClient, cluster_id: str, language: str):
    dbx_echo("Preparing execution context")
    context_id = create_context(v1_client, cluster_id, language)
    return context_id


# sometimes cluster is already in the status="RUNNING", however it couldn't yet provide execution context
# to make the execute command stable is such situations, we add retry handler.
@retry(tries=10, delay=5, backoff=5)
def create_context(v1_client, cluster_id, language):
    payload = v1_client.perform_query(method='POST',
                                      path='/contexts/create',
                                      data={'language': language, 'clusterId': cluster_id})
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
        raise Exception("Cluster is misconfigured and cannot be started, please check cluster settings at first")
    elif cluster_info["state"] in ["PENDING", "RESTARTING"]:
        dbx_echo("Cluster is getting prepared, current state: %s" % cluster_info["state"])
        time.sleep(10)
        awake_cluster(cluster_service, cluster_id)


def _define_language(path: pathlib.Path) -> str:
    suffix = path.suffix
    language = SUFFIX_MAPPING.get(suffix)

    if not language:
        raise Exception("Unexpected file extension: %s" % suffix)

    return language


def _preprocess_cluster_args(api_client: ApiClient, cluster_name, cluster_id) -> str:
    cluster_service = ClusterService(api_client)

    if not cluster_name and not cluster_id:
        raise Exception("Parameters cluster-name and cluster-id couldn't be empty at the same time.")

    if cluster_name:

        existing_clusters = cluster_service.list_clusters()["clusters"]
        matching_clusters = [c for c in existing_clusters if c["cluster_name"] == cluster_name]

        if not matching_clusters:
            raise Exception("No clusters with name %s found" % cluster_name)
        cluster_id = matching_clusters[0]["cluster_id"]

    if cluster_id:
        if not cluster_service.get_cluster(cluster_id):
            raise Exception("Cluster with id %s not found" % cluster_id)

    return cluster_id
