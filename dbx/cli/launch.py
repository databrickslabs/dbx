import base64
import json
from typing import List, Dict, Any

import click
import mlflow
import time
from databricks_cli.dbfs.api import DbfsService
from databricks_cli.jobs.api import JobsService
from databricks_cli.sdk.api_client import ApiClient

from dbx.cli.utils import dbx_echo, _parse_params, _generate_filter_string, \
    _provide_environment, _adjust_context


@click.command(context_settings=_adjust_context(),
               short_help="Launches job, choosing the latest version by given tags.")
@click.option("--environment", required=True, type=str, help="Environment name.")
@click.option("--job", required=True, type=str, help="Job name.")
@click.option("--trace", is_flag=True, help="Trace the job until it finishes.")
@click.argument('tags', nargs=-1, type=click.UNPROCESSED)
def launch(environment: str, job: str, trace: bool, tags: List[str]):
    deployment_tags = _parse_params(tags)
    dbx_echo("Launching job by given parameters")

    environment_data, api_client = _provide_environment(environment)

    filter_string = _generate_filter_string(environment, deployment_tags)

    runs = mlflow.search_runs(experiment_ids=environment_data["experiment_id"],
                              filter_string=filter_string,
                              max_results=1)

    if runs.empty:
        raise EnvironmentError("""
        No runs provided per given set of filters:
            %s
        Please check filters experiment UI to verify current status of deployments.
        """ % filter_string)

    run_info = runs.iloc[0].to_dict()

    deployment_run_id = run_info["run_id"]

    with mlflow.start_run(run_id=deployment_run_id) as deployment_run:
        with mlflow.start_run(nested=True):

            artifact_base_uri = deployment_run.info.artifact_uri
            deployments = _load_deployments(api_client, artifact_base_uri)
            job_id = deployments.get(job)

            if not job_id:
                raise Exception("Job with name %s not found in the latest deployment" % job)

            jobs_service = JobsService(api_client)
            run_data = jobs_service.run_now(job_id)

            if trace:
                dbx_status = _trace_run(api_client, run_data)
            else:
                dbx_status = "NOT_TRACKED"

            deployment_tags.update({
                "job_id": job_id,
                "run_id": run_data["run_id"],
                "dbx_action_type": "launch",
                "dbx_status": dbx_status,
                "dbx_environment": environment
            })

            mlflow.set_tags(deployment_tags)


def _load_deployments(api_client: ApiClient, artifact_base_uri: str):
    dbfs_service = DbfsService(api_client)
    dbx_deployments = "%s/.dbx/deployments.json" % artifact_base_uri
    raw_config_payload = dbfs_service.read(dbx_deployments)["data"]
    payload = base64.b64decode(raw_config_payload).decode("utf-8")
    deployments = json.loads(payload)
    return deployments


def _trace_run(api_client: ApiClient, run_data: Dict[str, Any]) -> str:
    dbx_echo("Tracing job run")
    while True:
        status = _get_run_status(api_client, run_data)
        result_state = status["state"].get("result_state", None)
        if result_state:
            if result_state == "SUCCESS":
                dbx_echo("Job run finished successfully")
                return "SUCCESS"
            else:
                return "ERROR"
        else:
            dbx_echo("Job run is not yet finished, current status message: %s" % status["state"]["state_message"])
            time.sleep(5)


def _get_run_status(api_client: ApiClient, run_data: Dict[str, Any]) -> Dict[str, Any]:
    run_status = api_client.perform_query('GET', '/jobs/runs/get', data={"run_id": run_data["run_id"]}, headers=None)
    return run_status
