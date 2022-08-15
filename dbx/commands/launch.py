import tempfile
import time
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from typing import List

import mlflow
import pandas as pd
import typer
from databricks_cli.jobs.api import JobsService
from databricks_cli.sdk.api_client import ApiClient
from mlflow.tracking import MlflowClient

from dbx.api.configure import ConfigurationManager
from dbx.api.output_provider import OutputProvider
from dbx.options import ENVIRONMENT_OPTION, TAGS_OPTION, BRANCH_NAME_OPTION
from dbx.utils import dbx_echo
from dbx.utils.common import (
    generate_filter_string,
    prepare_environment,
    parse_multiple,
    get_current_branch_name,
)
from dbx.utils.job_listing import find_job_by_name
from dbx.utils.json import JsonUtils

TERMINAL_RUN_LIFECYCLE_STATES = ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]
POSSIBLE_TASK_KEYS = ["notebook_task", "spark_jar_task", "spark_python_task", "spark_submit_task"]


def launch(
    environment: str = ENVIRONMENT_OPTION,
    job: str = typer.Option(..., "--job", help="[red]This option is deprecated[/red]"),
    trace: bool = typer.Option(False, "--trace", help="Trace the workload until it finishes.", is_flag=True),
    kill_on_sigterm: bool = typer.Option(
        False,
        "--kill-on-sigterm",
        is_flag=True,
        help="If provided, kills the job on SIGTERM (Ctrl+C) signal",
    ),
    existing_runs: str = typer.Option(
        "pass",
        "--existing-runs",
        help="""
        Strategy to handle existing active job runs.

        Options behaviour:

        * :code:`wait` will wait for all existing job runs to be finished
        * :code:`cancel` will cancel all existing job runs
        * :code:`pass` will simply pass the check and try to launch the job directly
        """,
    ),
    as_run_submit: bool = typer.Option(False, "--as-run-submit", is_flag=True, help="Run the job as run submit."),
    tags: Optional[List[str]] = TAGS_OPTION,
    branch_name: Optional[str] = BRANCH_NAME_OPTION,
    include_output: Optional[str] = typer.Option(
        None,
        "--include-output",
        help="""
        If provided, adds run output to the console output of the launch command.
        Please note that this option is only supported for Jobs V2.X+.
        For jobs created without tasks section output won't be printed.
        If not provided, run output will be omitted.

        Options behaviour:

        * :code:`stdout` will add stdout and stderr to the console output
        * :code:`stderr` will add only stderr to the console output
        """,
    ),
):
    dbx_echo(f"Launching job {job} on environment {environment}")

    api_client = prepare_environment(environment)
    additional_tags = parse_multiple(tags)

    if not branch_name:
        branch_name = get_current_branch_name()

    filter_string = generate_filter_string(environment, branch_name)

    run_info = _find_deployment_run(filter_string, additional_tags, as_run_submit, environment)

    deployment_run_id = run_info["run_id"]

    with mlflow.start_run(run_id=deployment_run_id):

        with mlflow.start_run(nested=True):

            if not as_run_submit:
                run_launcher = RunNowLauncher(job=job, api_client=api_client, existing_runs=existing_runs)
            else:
                run_launcher = RunSubmitLauncher(
                    job=job,
                    api_client=api_client,
                    existing_runs=existing_runs,
                    deployment_run_id=deployment_run_id,
                    environment=environment,
                )

            run_data, job_id = run_launcher.launch()

            jobs_service = JobsService(api_client)
            run_info = jobs_service.get_run(run_data["run_id"])
            run_url = run_info.get("run_page_url")
            dbx_echo(f"Run URL: {run_url}")
            if trace:
                if kill_on_sigterm:
                    dbx_echo("Click Ctrl+C to stop the run")
                    try:
                        dbx_status, final_run_state = _trace_run(api_client, run_data)
                    except KeyboardInterrupt:
                        dbx_status = "CANCELLED"
                        dbx_echo("Cancelling the run gracefully")
                        _cancel_run(api_client, run_data)
                        dbx_echo("Run cancelled successfully")
                else:
                    dbx_status, final_run_state = _trace_run(api_client, run_data)

                dbx_echo("Launch command finished")

                if include_output:
                    log_provider = OutputProvider(jobs_service, final_run_state)
                    dbx_echo(f"Run output provisioning requested with level {include_output}")
                    log_provider.provide(include_output)

                if dbx_status == "ERROR":
                    raise Exception(
                        "Tracked run failed during execution. "
                        "Please check the status and logs of the run for details."
                    )
            else:
                dbx_status = "NOT_TRACKED"
                dbx_echo(
                    "Run successfully launched in non-tracking mode :rocket:. "
                    "Please check Databricks UI for job status :eyes:"
                )

            deployment_tags = {
                "job_id": job_id,
                "run_id": run_data.get("run_id"),
                "dbx_action_type": "launch",
                "dbx_status": dbx_status,
                "dbx_environment": environment,
            }

            if branch_name:
                deployment_tags["dbx_branch_name"] = branch_name

            mlflow.set_tags(deployment_tags)


def _find_deployment_run(
    filter_string: str, tags: Dict[str, str], as_run_submit: bool, environment: str
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

    if as_run_submit:
        if "tags.dbx_deploy_type" not in _runs.columns:
            raise Exception(
                """"
                Run Submit API is available only when deployment was done with --files-only flag.
                Currently there is no deployments with such flag under given filters.
                Please re-deploy with --files-only flag and then re-run this launch command.
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
        if as_run_submit:
            exception_string = (
                exception_string
                + """
            With file-based deployments (dbx_deployment_type='files_only')."""
            )

        experiment_location = ConfigurationManager().get(environment).workspace_dir
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


class RunSubmitLauncher:
    def __init__(
        self,
        job: str,
        api_client: ApiClient,
        deployment_run_id: str,
        existing_runs: str,
        environment: str,
    ):
        self.run_id = deployment_run_id
        self.job = job
        self.api_client = api_client
        self.existing_runs = existing_runs
        self.environment = environment

    def launch(self) -> Tuple[Dict[Any, Any], Optional[str]]:
        dbx_echo("Launching job via run submit API")

        env_spec = _load_dbx_file(self.run_id, "deployment-result.json").get(self.environment)

        if not env_spec:
            raise Exception(f"No job definitions found for environment {self.environment}")

        job_specs = env_spec.get("jobs")

        found_jobs = [j for j in job_specs if j["name"] == self.job]

        if not found_jobs:
            raise Exception(f"Job definition {self.job} not found in deployment spec")

        job_spec: Dict[str, Any] = found_jobs[0]

        run_data = _submit_run(self.api_client, job_spec)
        return run_data, None


class RunNowLauncher:
    def __init__(self, job: str, api_client: ApiClient, existing_runs: str):
        self.job = job
        self.api_client = api_client
        self.existing_runs = existing_runs

    def launch(self) -> Tuple[Dict[Any, Any], Optional[str]]:
        dbx_echo("Launching job via run now API")
        jobs_service = JobsService(self.api_client)
        job_data = find_job_by_name(jobs_service, self.job)

        if not job_data:
            raise Exception(f"Job with name {self.job} not found")

        job_id = job_data["job_id"]

        active_runs = jobs_service.list_runs(job_id, active_only=True).get("runs", [])

        for run in active_runs:
            if self.existing_runs == "pass":
                dbx_echo("Passing the existing runs status check")

            if self.existing_runs == "wait":
                dbx_echo(f'Waiting for job run with id {run["run_id"]} to be finished')
                _wait_run(self.api_client, run)

            if self.existing_runs == "cancel":
                dbx_echo(f'Cancelling run with id {run["run_id"]}')
                _cancel_run(self.api_client, run)

        else:
            run_data = jobs_service.run_now(job_id)

        return run_data, job_id


def _define_payload_key(job_settings: Dict[str, Any]):
    if job_settings.get("notebook_task"):
        extra_payload_key = "notebook_params"
    elif job_settings.get("spark_jar_task"):
        extra_payload_key = "jar_params"
    elif job_settings.get("spark_python_task"):
        extra_payload_key = "python_params"
    elif job_settings.get("spark_submit_task"):
        extra_payload_key = "spark_submit_params"
    else:
        raise Exception(f"Unexpected type of the job with settings: {job_settings}")

    return extra_payload_key


def _submit_run(api_client: ApiClient, payload: Dict[str, Any]) -> Dict[str, Any]:
    return api_client.perform_query("POST", "/jobs/runs/submit", data=payload)


def _cancel_run(api_client: ApiClient, run_data: Dict[str, Any]):
    jobs_service = JobsService(api_client)
    jobs_service.cancel_run(run_data["run_id"])
    _wait_run(api_client, run_data)


def _load_dbx_file(run_id: str, file_name: str) -> Dict[Any, Any]:
    client = MlflowClient()
    with tempfile.TemporaryDirectory() as tmp:
        dbx_file_path = f".dbx/{file_name}"
        client.download_artifacts(run_id, dbx_file_path, tmp)
        return JsonUtils.read(Path(tmp) / dbx_file_path)


def _wait_run(api_client: ApiClient, run_data: Dict[str, Any]) -> Dict[str, Any]:
    dbx_echo(f"Tracing run with id {run_data['run_id']}")
    while True:
        time.sleep(5)  # runs API is eventually consistent, it's better to have a short pause for status update
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


def _trace_run(api_client: ApiClient, run_data: Dict[str, Any]) -> [str, Dict[str, Any]]:
    final_status = _wait_run(api_client, run_data)
    result_state = final_status["state"].get("result_state", None)
    if result_state == "SUCCESS":
        dbx_echo("Job run finished successfully")
        return "SUCCESS", final_status
    else:
        return "ERROR", final_status


def _get_run_status(api_client: ApiClient, run_data: Dict[str, Any]) -> Dict[str, Any]:
    jobs_service = JobsService(api_client)
    run_status = jobs_service.get_run(run_data["run_id"])
    return run_status
