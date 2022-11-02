import time
from typing import Dict, Any, List

import mlflow
from databricks_cli.sdk import ApiClient, JobsService
from mlflow.entities import Run
from rich.console import Console

from dbx.api.configure import ProjectConfigurationManager
from dbx.api.launch.runners.base import RunData
from dbx.constants import TERMINAL_RUN_LIFECYCLE_STATES
from dbx.utils import dbx_echo, format_dbx_message


def cancel_run(api_client: ApiClient, run_data: RunData):
    jobs_service = JobsService(api_client)
    jobs_service.cancel_run(run_data.run_id)
    wait_run(api_client, run_data)


def wait_run(api_client: ApiClient, run_data: RunData) -> Dict[str, Any]:
    with Console().status(
        format_dbx_message(f"Tracing run with id {run_data.run_id}"), spinner="dots"
    ) as console_status:
        while True:
            time.sleep(5)  # runs API is eventually consistent, it's better to have a short pause for status update
            status = get_run_status(api_client, run_data)
            run_state = status["state"]
            life_cycle_state = run_state.get("life_cycle_state", None)

            console_status.update(format_dbx_message(f"[Run Id: {run_data.run_id}] run state: {run_state}"))

            if life_cycle_state in TERMINAL_RUN_LIFECYCLE_STATES:
                return status


def get_run_status(api_client: ApiClient, run_data: RunData) -> Dict[str, Any]:
    jobs_service = JobsService(api_client)
    run_status = jobs_service.get_run(run_data.run_id)
    return run_status


def find_deployment_run(filter_string: str, tags: Dict[str, str], from_assets: bool, environment: str) -> Run:
    runs: List[Run] = mlflow.search_runs(filter_string=filter_string, output_format="list")

    def _filter_by_tags(run: Run) -> bool:
        return all(run.data.tags.get(tag_name) == tag_value for tag_name, tag_value in tags.items())

    if tags:
        dbx_echo("Filtering deployments with set of additional tags")
        runs = list(filter(_filter_by_tags, runs))
    else:
        dbx_echo("No additional tags provided")

    if from_assets:
        runs = list(filter(lambda r: r.data.tags.get("dbx_deploy_type") == "files_only", runs))

    if not runs:
        exception_string = f"""
        No deployments provided per given set of filters:
            {filter_string}"""
        if tags:
            exception_string = (
                exception_string
                + f"""
            With additional tags: {tags}"""
            )
        if from_assets:
            exception_string = (
                exception_string
                + """
            With asset-based deployments (dbx_deployment_type='files_only')."""
            )

        experiment_location = ProjectConfigurationManager().get(environment).properties.workspace_directory
        exception_string = (
            exception_string
            + f"""
        To verify current status of deployments please check experiment UI in workspace dir: {experiment_location}
        """
        )

        raise Exception(exception_string)

    last_run_info = sorted(runs, key=lambda r: r.info.start_time, reverse=True)[0]

    dbx_echo("Successfully found deployment per given job name")
    return last_run_info


def trace_run(api_client: ApiClient, run_data: RunData) -> [str, Dict[str, Any]]:
    final_status = wait_run(api_client, run_data)
    dbx_echo(f"Finished tracing run with id {run_data.run_id}")
    result_state = final_status["state"].get("result_state", None)
    if result_state == "SUCCESS":
        dbx_echo("Job run finished successfully")
        return "SUCCESS", final_status
    else:
        return "ERROR", final_status
