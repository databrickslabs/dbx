import tempfile
import time
from pathlib import Path
from typing import Dict, Any

import mlflow
import pandas as pd

from databricks_cli.sdk import ApiClient, JobsService
from mlflow.tracking import MlflowClient

from dbx.api.configure import ProjectConfigurationManager
from dbx.constants import TERMINAL_RUN_LIFECYCLE_STATES
from dbx.utils import dbx_echo

from dbx.utils.json import JsonUtils


def cancel_run(api_client: ApiClient, run_data: Dict[str, Any]):
    jobs_service = JobsService(api_client)
    jobs_service.cancel_run(run_data["run_id"])
    wait_run(api_client, run_data)


def load_dbx_file(run_id: str, file_name: str) -> Dict[Any, Any]:
    client = MlflowClient()
    with tempfile.TemporaryDirectory() as tmp:
        dbx_file_path = f".dbx/{file_name}"
        client.download_artifacts(run_id, dbx_file_path, tmp)
        return JsonUtils.read(Path(tmp) / dbx_file_path)


def wait_run(api_client: ApiClient, run_data: Dict[str, Any]) -> Dict[str, Any]:
    dbx_echo(f"Tracing run with id {run_data['run_id']}")
    while True:
        time.sleep(5)  # runs API is eventually consistent, it's better to have a short pause for status update
        status = get_run_status(api_client, run_data)
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


def get_run_status(api_client: ApiClient, run_data: Dict[str, Any]) -> Dict[str, Any]:
    jobs_service = JobsService(api_client)
    run_status = jobs_service.get_run(run_data["run_id"])
    return run_status


def find_deployment_run(
    filter_string: str, tags: Dict[str, str], from_assets: bool, environment: str
) -> Dict[str, Any]:
    runs = mlflow.search_runs(filter_string=filter_string)

    filter_conditions = []

    if tags:
        dbx_echo("Filtering deployments with set of additional tags")
        for tag_name, tag_value in tags.items():
            tag_column_name = f"tags.{tag_name}"
            if tag_column_name not in runs.columns:
                raise Exception(
                    f"Tag {tag_name} not found in underlying MLflow experiment, please verify tag existence in the UI"
                )
            tag_condition = runs[tag_column_name] == tag_value
            filter_conditions.append(tag_condition)
        full_filter = pd.DataFrame(filter_conditions).T.all(axis=1)  # noqa
        _runs = runs[full_filter]
    else:
        dbx_echo("No additional tags provided")
        _runs = runs

    if from_assets:
        if "tags.dbx_deploy_type" not in _runs.columns:
            raise Exception(
                """"
                Run Submit API is available only when deployment was done with --assets-only flag.
                Currently there is no deployments with such flag under given filters.
                Please re-deploy with --assets-only flag and then re-run this launch command.
            """
            )

        _runs = _runs[_runs["tags.dbx_deploy_type"] == "files_only"]

    if _runs.empty:
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

    run_info = _runs.iloc[0].to_dict()

    dbx_echo("Successfully found deployment per given job name")
    return run_info


def trace_run(api_client: ApiClient, run_data: Dict[str, Any]) -> [str, Dict[str, Any]]:
    final_status = wait_run(api_client, run_data)
    result_state = final_status["state"].get("result_state", None)
    if result_state == "SUCCESS":
        dbx_echo("Job run finished successfully")
        return "SUCCESS", final_status
    else:
        return "ERROR", final_status
