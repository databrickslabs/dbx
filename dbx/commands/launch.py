import base64
import json
import time
from typing import Dict, Any, Optional, Tuple
from typing import List

import click
import mlflow
import pandas as pd
from databricks_cli.configure.config import debug_option
from databricks_cli.dbfs.api import DbfsService
from databricks_cli.jobs.api import JobsService
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.utils.common import (
    dbx_echo,
    generate_filter_string,
    prepare_environment,
    environment_option,
    parse_multiple,
    InfoFile,
    get_current_branch_name,
)

TERMINAL_RUN_LIFECYCLE_STATES = ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]
POSSIBLE_TASK_KEYS = ["notebook_task", "spark_jar_task", "spark_python_task", "spark_submit_task"]


@click.command(
    context_settings=CONTEXT_SETTINGS,
    short_help="Launch the job by it's name on the given environment.",
    help="""
    Finds the job deployment and launches it on a automated or interactive cluster.
    
    This command will launch the given job by it's name on a given environment. 
    
    .. note::
        Job shall be deployed prior to be launched.
     
    """,
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
@click.option("--as-run-submit", is_flag=True, help="Run the job as run submit.")
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
            Format: (:code:`--parameters="parameter1=value1"`). 
            Option might be repeated multiple times.""",
)
@click.option(
    "--parameters-raw",
    type=str,
    help="""Parameters of the job as a raw string. \n
            If provided, default job arguments will be overridden.
            If provided, :code:`--parameters` argument will be ignored.
            Example command:
            :code:`dbx launch --job="my-job-name" --parameters-raw='{"key1": "value1", "key2": 2}'`.
            Please note that no parameters preprocessing will be done.
            """,
)
@click.option(
    "--branch-name",
    type=str,
    default=None,
    required=False,
    help="""The name of the current branch.
              If not provided or empty, dbx will try to detect the branch name.""",
)
@environment_option
@debug_option
def launch(
    environment: str,
    job: str,
    trace: bool,
    kill_on_sigterm: bool,
    existing_runs: str,
    as_run_submit: bool,
    tags: List[str],
    parameters: List[str],
    parameters_raw: Optional[str],
    branch_name: Optional[str],
):
    dbx_echo(f"Launching job {job} on environment {environment}")

    api_client = prepare_environment(environment)
    additional_tags = parse_multiple(tags)

    if not branch_name:
        branch_name = get_current_branch_name()

    if parameters_raw:
        prepared_parameters = parameters_raw
    else:
        override_parameters = parse_multiple(parameters)
        prepared_parameters = sum([[k, v] for k, v in override_parameters.items()], [])

    filter_string = generate_filter_string(environment)

    run_info = _find_deployment_run(filter_string, additional_tags, as_run_submit, environment)

    deployment_run_id = run_info["run_id"]

    with mlflow.start_run(run_id=deployment_run_id) as deployment_run:

        with mlflow.start_run(nested=True):
            artifact_base_uri = deployment_run.info.artifact_uri

            if not as_run_submit:
                run_launcher = RunNowLauncher(job, api_client, artifact_base_uri, existing_runs, prepared_parameters)
            else:
                run_launcher = RunSubmitLauncher(
                    job, api_client, artifact_base_uri, existing_runs, prepared_parameters, environment
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
                        dbx_status = _trace_run(api_client, run_data)
                    except KeyboardInterrupt:
                        dbx_status = "CANCELLED"
                        dbx_echo("Cancelling the run gracefully")
                        _cancel_run(api_client, run_data)
                        dbx_echo("Run cancelled successfully")
                else:
                    dbx_status = _trace_run(api_client, run_data)

                if dbx_status == "ERROR":
                    raise Exception("Tracked run failed during execution. Please check Databricks UI for run logs")
                dbx_echo("Launch command finished")

            else:
                dbx_status = "NOT_TRACKED"
                dbx_echo("Run successfully launched in non-tracking mode. Please check Databricks UI for job status")

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
    runs = mlflow.search_runs(filter_string=filter_string, order_by=["start_time DESC"])

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
        exception_string = """
        No runs provided per given set of filters:
            {filter_string}
        """
        if tags:
            exception_string = (
                exception_string
                + f"""
            With additional tags: {tags}
            """
            )
        if as_run_submit:
            exception_string = (
                exception_string
                + """
            With file-based deployments (dbx_deployment_type='files_only').
            """
            )
        experiment_location = InfoFile.get(environment).get("workspace_dir")
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
        artifact_base_uri: str,
        existing_runs: str,
        prepared_parameters: Any,
        environment: str,
    ):
        self.job = job
        self.api_client = api_client
        self.artifact_base_uri = artifact_base_uri
        self.existing_runs = existing_runs
        self.prepared_parameters = prepared_parameters
        self.environment = environment

    def launch(self) -> Tuple[Dict[Any, Any], Optional[str]]:
        dbx_echo("Launching job via run submit API")

        env_spec = _load_dbx_file(self.api_client, self.artifact_base_uri, "deployment-result.json").get(
            self.environment
        )

        if not env_spec:
            raise Exception(f"No job definitions found for environment {self.environment}")

        job_specs = env_spec.get("jobs")

        found_jobs = [j for j in job_specs if j["name"] == self.job]

        if not found_jobs:
            raise Exception(f"Job definition {self.job} not found in deployment spec")

        job_spec: Dict[str, Any] = found_jobs[0]

        if self.prepared_parameters:
            task_key = [k for k in job_spec.keys() if k in POSSIBLE_TASK_KEYS][0]
            job_spec[task_key]["parameters"] = self.prepared_parameters

        run_data = _submit_run(self.api_client, job_spec)
        return run_data, None


class RunNowLauncher:
    def __init__(
        self, job: str, api_client: ApiClient, artifact_base_uri: str, existing_runs: str, prepared_parameters: Any
    ):
        self.job = job
        self.api_client = api_client
        self.artifact_base_uri = artifact_base_uri
        self.existing_runs = existing_runs
        self.prepared_parameters = prepared_parameters

    def launch(self) -> Tuple[Dict[Any, Any], Optional[str]]:
        dbx_echo("Launching job via run now API")
        jobs_service = JobsService(self.api_client)

        all_jobs = jobs_service.list_jobs().get("jobs", [])

        matching_jobs = [j for j in all_jobs if j["settings"]["name"] == self.job]

        if not matching_jobs:
            raise Exception(f"Job with name {self.job} not found")

        if len(matching_jobs) > 1:
            raise Exception(f"Job with name {self.job} is duplicated. Please make job name unique.")

        job_data = matching_jobs[0]
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

        if self.prepared_parameters:
            dbx_echo(f"Default launch parameters are overridden with the following: {self.prepared_parameters}")
            # we don't do a null-check here since the job existence will be already done during listing above.
            job_settings = job_data.get("settings")

            # here we define the job type to correctly pass parameters
            extra_payload_key = _define_payload_key(job_settings)

            extra_payload = {extra_payload_key: self.prepared_parameters}

            run_data = jobs_service.run_now(job_id, **extra_payload)

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


def _load_dbx_file(api_client: ApiClient, artifact_base_uri: str, name: str) -> Dict[Any, Any]:
    dbfs_service = DbfsService(api_client)
    dbx_deployments = f"{artifact_base_uri}/.dbx/{name}"
    raw_config_payload = dbfs_service.read(dbx_deployments)["data"]
    payload = base64.b64decode(raw_config_payload).decode("utf-8")
    deployments = json.loads(payload)
    return deployments


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
