from typing import List
from typing import Optional

import mlflow
import typer
from databricks_cli.jobs.api import JobsService

from dbx.api.launch.functions import find_deployment_run
from dbx.api.launch.runners import RunSubmitLauncher, RunNowLauncher
from dbx.api.launch.tracer import RunTracer
from dbx.api.output_provider import OutputProvider
from dbx.models.options import ExistingRunsOption, IncludeOutputOption
from dbx.options import (
    ENVIRONMENT_OPTION,
    TAGS_OPTION,
    BRANCH_NAME_OPTION,
    DEBUG_OPTION,
    WORKFLOW_ARGUMENT,
    LAUNCH_PARAMETERS_OPTION,
)
from dbx.utils import dbx_echo
from dbx.utils.common import (
    generate_filter_string,
    prepare_environment,
    parse_multiple,
    get_current_branch_name,
)


def launch(
    workflow: str = WORKFLOW_ARGUMENT,
    environment: str = ENVIRONMENT_OPTION,
    job: str = typer.Option(
        None,
        "--job",
        help="This option is deprecated, please use workflow name as argument instead.",
        show_default=False,
    ),
    trace: bool = typer.Option(False, "--trace", help="Trace the workload until it finishes.", is_flag=True),
    kill_on_sigterm: bool = typer.Option(
        False,
        "--kill-on-sigterm",
        is_flag=True,
        help="If provided, kills the job on SIGTERM (Ctrl+C) signal.",
    ),
    existing_runs: ExistingRunsOption = typer.Option(
        ExistingRunsOption.pass_.value,
        "--existing-runs",
        help="""
        Strategy to handle existing active job runs.
        Option will only work in case if workload is launched as a job.

        Options behaviour:

        `wait` will wait for all existing job runs to be finished

        `cancel` will cancel all existing job runs

        `pass` will pass the check and launch the workflow""",
    ),
    as_run_submit: bool = typer.Option(
        False,
        "--as-run-submit",
        is_flag=True,
        help="This option is deprecated, please use `--from-assets` flag instead.",
    ),
    from_assets: bool = typer.Option(
        False,
        "--from-assets",
        is_flag=True,
        is_eager=True,
        help="""
        Creates a one-time run using assets deployed with `dbx deploy --assets-only` option.

        Please note that one-time run is created using RunSubmit API.

        🚨 This workflow run won't be visible in the Jobs UI, but it will be visible in the Jobs Run tab.

        Shared job cluster feature is not supported in runs/submit API and therefore is not supported with this flag.
        """,
    ),
    tags: Optional[List[str]] = TAGS_OPTION,
    branch_name: Optional[str] = BRANCH_NAME_OPTION,
    include_output: Optional[IncludeOutputOption] = typer.Option(
        None,
        "--include-output",
        help="""If provided, adds run output to the console output of the launch command.

        ℹ️ Please note that this option is only supported for Jobs V2.X+.

        For jobs created without tasks section output won't be printed.
        If not provided, run output will be omitted.

        Options behaviour:

        `stdout` will add both stdout and stderr to the console output

        `stderr` will add only stderr to the console output""",
    ),
    parameters: Optional[str] = LAUNCH_PARAMETERS_OPTION,
    debug: Optional[bool] = DEBUG_OPTION,  # noqa
):
    _job = workflow if workflow else job

    if not _job:
        raise Exception("Please provide workflow name as an argument")

    dbx_echo(f"Launching job {_job} on environment {environment}")

    api_client = prepare_environment(environment)
    additional_tags = parse_multiple(tags)

    if not branch_name:
        branch_name = get_current_branch_name()

    filter_string = generate_filter_string(environment, branch_name)
    _from_assets = from_assets if from_assets else as_run_submit

    last_deployment_run = find_deployment_run(filter_string, additional_tags, _from_assets, environment)

    with mlflow.start_run(run_id=last_deployment_run.info.run_id):

        with mlflow.start_run(nested=True):

            if not _from_assets:
                run_launcher = RunNowLauncher(
                    job=_job, api_client=api_client, existing_runs=existing_runs, parameters=parameters
                )
            else:
                run_launcher = RunSubmitLauncher(
                    job=_job,
                    api_client=api_client,
                    deployment_run_id=last_deployment_run.info.run_id,
                    environment=environment,
                    parameters=parameters,
                )

            run_data, job_id = run_launcher.launch()

            jobs_service = JobsService(api_client)
            run_info = jobs_service.get_run(run_data["run_id"])
            run_url = run_info.get("run_page_url")
            dbx_echo(f"Run URL: {run_url}")
            if trace:
                dbx_status, final_run_state = RunTracer.start(kill_on_sigterm, api_client, run_data)
                if include_output:
                    log_provider = OutputProvider(jobs_service, final_run_state)
                    dbx_echo(f"Run output provisioning requested with level {include_output.value}")
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
