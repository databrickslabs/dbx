import base64
import json
import time
from typing import Dict, Any
from typing import List

import click
import mlflow
from databricks_cli.dbfs.api import DbfsService
from databricks_cli.jobs.api import JobsService
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.utils import CONTEXT_SETTINGS
from databricks_cli.configure.config import debug_option

from dbx.utils.common import (
    dbx_echo,
    generate_filter_string,
    prepare_environment,
    environment_option,
    parse_multiple,
)

TERMINAL_RUN_LIFECYCLE_STATES = ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]


@click.command(
    context_settings=CONTEXT_SETTINGS,
    short_help="Launch the job by it's name on the given environment.",
    help="""
    Finds the job deployment and launches it on a automated or interactive cluster.
    
    This command will launch the given job by it's name on a given environment. 
    
    .. note::
        Job shall be deployed prior to be launched.
     
    """
)
@click.option("--job", required=True, type=str, help="Job name.")
@click.option("--trace", is_flag=True, help="Trace the job until it finishes.")
@click.option(
    "--kill-on-sigterm",
    is_flag=True,
    help="If provided, kills the job on SIGTERM (Ctrl+C) signal",
)
@click.option(
    "--existing-runs",
    type=click.Choice(["wait", "cancel", "pass"]),
    default="pass",
    help="""
        Strategy to handle existing active job runs. 
        
        Options behaviour:
        
        * :code:`wait` will wait for all existing job runs to be finished
        * :code:`cancel` will cancel all existing job runs
        * :code:`pass` will simply pass the check and try to launch the job directly
        """,
)
@click.option(
    "--tags",
    multiple=True,
    type=str,
    help="""Additional tags to search for the latest deployment.
              Format: (:code:`--tags="tag_name=tag_value"`). 
              Option might be repeated multiple times.""",
)
@click.option(
    "--parameters",
    multiple=True,
    type=str,
    help="""Parameters of the job. \n
            If provided, default job arguments will be overridden.
            Option might be repeated multiple times.""",
)
@environment_option
@debug_option
def launch(
        environment: str,
        job: str,
        trace: bool,
        kill_on_sigterm: bool,
        existing_runs: str,
        tags: List[str],
        parameters: List[str],
):
    dbx_echo(f"Launching job {job} on environment {environment}")

    api_client = prepare_environment(environment)
    additional_tags = parse_multiple(tags)
    override_parameters = parse_multiple(parameters)

    filter_string = generate_filter_string(environment, additional_tags)

    runs = mlflow.search_runs(filter_string=filter_string, max_results=1)

    if runs.empty:
        raise EnvironmentError(
            f"""
        No runs provided per given set of filters:
            {filter_string}
        Please check experiment UI to verify current status of deployments.
        """
        )

    run_info = runs.iloc[0].to_dict()

    dbx_echo("Successfully found deployment per given job name")

    deployment_run_id = run_info["run_id"]

    with mlflow.start_run(run_id=deployment_run_id) as deployment_run:
        with mlflow.start_run(nested=True):

            artifact_base_uri = deployment_run.info.artifact_uri
            deployments = _load_deployments(api_client, artifact_base_uri)
            job_id = deployments.get(job)

            if not job_id:
                raise Exception(
                    f"Job with name {job} not found in the latest deployment" % job
                )

            jobs_service = JobsService(api_client)
            active_runs = jobs_service.list_runs(job_id, active_only=True).get(
                "runs", []
            )

            for run in active_runs:
                if existing_runs == "pass":
                    dbx_echo("Passing the existing runs status check")

                if existing_runs == "wait":
                    dbx_echo(
                        f'Waiting for job run with id {run["run_id"]} to be finished'
                    )
                    _wait_run(api_client, run)

                if existing_runs == "cancel":
                    dbx_echo(f'Cancelling run with id {run["run_id"]}')
                    _cancel_run(api_client, run)

            if override_parameters:
                _prepared_parameters = sum(
                    [[k, v] for k, v in override_parameters.items()], []
                )
                dbx_echo(
                    f"Default launch parameters are overridden with the following: {_prepared_parameters}"
                )
                run_data = jobs_service.run_now(
                    job_id, python_params=_prepared_parameters
                )
            else:
                run_data = jobs_service.run_now(job_id)

            if trace:
                dbx_echo("Tracing job run")
                if kill_on_sigterm:
                    dbx_echo("Click Ctrl+C to stop the job run")
                    try:
                        dbx_status = _trace_run(api_client, run_data)
                    except KeyboardInterrupt:
                        dbx_status = "CANCELLED"
                        dbx_echo("Cancelling the run gracefully")
                        _cancel_run(api_client, run_data)
                        dbx_echo("Run cancelled successfully")
                else:
                    dbx_status = _trace_run(api_client, run_data)

                if dbx_status == "ERROR":
                    raise Exception(
                        "Tracked job failed during execution. "
                        "Please check Databricks UI for job logs"
                    )
                dbx_echo("Launch command finished")

            else:
                dbx_status = "NOT_TRACKED"
                dbx_echo(
                    "Job successfully launched in non-tracking mode. Please check Databricks UI for job status"
                )

            deployment_tags = {
                "job_id": job_id,
                "run_id": run_data["run_id"],
                "dbx_action_type": "launch",
                "dbx_status": dbx_status,
                "dbx_environment": environment,
            }

            mlflow.set_tags(deployment_tags)


def _cancel_run(api_client: ApiClient, run_data: Dict[str, Any]):
    jobs_service = JobsService(api_client)
    jobs_service.cancel_run(run_data["run_id"])
    _wait_run(api_client, run_data)


def _load_deployments(api_client: ApiClient, artifact_base_uri: str):
    dbfs_service = DbfsService(api_client)
    dbx_deployments = f"{artifact_base_uri}/.dbx/deployments.json"
    raw_config_payload = dbfs_service.read(dbx_deployments)["data"]
    payload = base64.b64decode(raw_config_payload).decode("utf-8")
    deployments = json.loads(payload)
    return deployments


def _wait_run(api_client: ApiClient, run_data: Dict[str, Any]) -> Dict[str, Any]:
    dbx_echo(f"Tracing run with id {run_data['run_id']}")
    while True:
        time.sleep(
            5
        )  # runs API is eventually consistent, it's better to have a short pause for status update
        status = _get_run_status(api_client, run_data)
        run_state = status["state"]
        result_state = run_state.get("result_state", None)
        life_cycle_state = run_state.get("life_cycle_state", None)
        state_message = run_state.get("state_message")

        dbx_echo(
            f"[Run Id: {run_data['run_id']}] Current run status info - "
            f"result state: {result_state}, "
            f"lifecycle state: {life_cycle_state}, "
            f"state message: {state_message}"
        )

        if life_cycle_state in TERMINAL_RUN_LIFECYCLE_STATES:
            dbx_echo(f"Finished tracing run with id {run_data['run_id']}")
            return status


def _trace_run(api_client: ApiClient, run_data: Dict[str, Any]) -> str:
    final_status = _wait_run(api_client, run_data)
    result_state = final_status["state"].get("result_state", None)
    if result_state == "SUCCESS":
        dbx_echo("Job run finished successfully")
        return "SUCCESS"
    else:
        return "ERROR"


def _get_run_status(api_client: ApiClient, run_data: Dict[str, Any]) -> Dict[str, Any]:
    jobs_service = JobsService(api_client)
    run_status = jobs_service.get_run(run_data["run_id"])
    return run_status
