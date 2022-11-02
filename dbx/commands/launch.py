from typing import List, Dict, Any
from typing import Optional

import mlflow
import typer
from databricks_cli.jobs.api import JobsService
from rich.markup import escape

from dbx.api.launch.functions import find_deployment_run
from dbx.api.launch.pipeline_models import PipelineUpdateState
from dbx.api.launch.runners.asset_based import AssetBasedLauncher
from dbx.api.launch.runners.base import RunData
from dbx.api.launch.runners.pipeline import PipelineLauncher
from dbx.api.launch.runners.standard import StandardLauncher
from dbx.api.launch.tracer import RunTracer, PipelineTracer
from dbx.api.output_provider import OutputProvider
from dbx.models.cli.options import ExistingRunsOption, IncludeOutputOption
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
    workflow_name: str = WORKFLOW_ARGUMENT,
    environment_name: str = ENVIRONMENT_OPTION,
    job_name: str = typer.Option(
        None,
        "--job",
        help="This option is deprecated, please use workflow name as argument instead.",
        show_default=False,
    ),
    is_pipeline: bool = typer.Option(
        False,
        "--pipeline",
        "-p",
        is_flag=True,
        help="Search for the workflow in the DLT pipelines instead of standard job objects.",
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
    workflow_name = workflow_name if workflow_name else job_name

    if not workflow_name:
        raise Exception("Please provide workflow name as an argument")

    if is_pipeline and from_assets:
        raise Exception("DLT pipelines cannot be launched in the asset-based mode")

    dbx_echo(f"Launching workflow {escape(workflow_name)} on environment {environment_name}")

    api_client = prepare_environment(environment_name)
    additional_tags = parse_multiple(tags)

    if not branch_name:
        branch_name = get_current_branch_name()

    filter_string = generate_filter_string(environment_name, branch_name)
    _from_assets = from_assets if from_assets else as_run_submit

    last_deployment_run = find_deployment_run(filter_string, additional_tags, _from_assets, environment_name)

    with mlflow.start_run(run_id=last_deployment_run.info.run_id):

        with mlflow.start_run(nested=True):

            if is_pipeline:
                launcher = PipelineLauncher(workflow_name=workflow_name, api_client=api_client, parameters=parameters)
            else:
                if not _from_assets:
                    launcher = StandardLauncher(
                        workflow_name=workflow_name,
                        api_client=api_client,
                        existing_runs=existing_runs,
                        parameters=parameters,
                    )
                else:
                    launcher = AssetBasedLauncher(
                        workflow_name=workflow_name,
                        api_client=api_client,
                        deployment_run_id=last_deployment_run.info.run_id,
                        environment_name=environment_name,
                        parameters=parameters,
                    )

            process_info, object_id = launcher.launch()

            if isinstance(process_info, RunData):
                jobs_service = JobsService(api_client)
                run_info = jobs_service.get_run(process_info.run_id)
                run_url = run_info.get("run_page_url")
                dbx_echo(f"Run URL: {run_url}")
            else:
                dbx_echo("DLT pipeline launched successfully")

        if trace:
            if isinstance(process_info, RunData):
                status = trace_workflow_object(api_client, process_info, include_output, kill_on_sigterm)
                additional_tags = {
                    "job_id": object_id,
                    "run_id": process_info.run_id,
                }
            else:
                final_state = PipelineTracer.start(
                    api_client=api_client, process_info=process_info, pipeline_id=object_id
                )
                if final_state == PipelineUpdateState.FAILED:
                    raise Exception(
                        f"Tracked pipeline {object_id} failed during execution, please check the UI for details."
                    )
                status = final_state
                additional_tags = {"pipeline_id": object_id}
        else:
            status = "NOT_TRACKED"
            dbx_echo(
                "Workflow successfully launched in the non-tracking mode 🚀. "
                "Please check Databricks UI for job status 👀"
            )
        log_launch_info(additional_tags, status, environment_name, branch_name)


def trace_workflow_object(
    api_client,
    run_data: RunData,
    include_output,
    kill_on_sigterm,
):
    dbx_status, final_run_state = RunTracer.start(kill_on_sigterm, api_client, run_data)
    js = JobsService(api_client)
    if include_output:
        log_provider = OutputProvider(js, final_run_state)
        dbx_echo(f"Run output provisioning requested with level {include_output.value}")
        log_provider.provide(include_output)

    if dbx_status == "ERROR":
        raise Exception("Tracked run failed during execution. Please check the status and logs of the run for details.")
    return dbx_status


def log_launch_info(additional_tags: Dict[str, Any], dbx_status, environment_name, branch_name):
    deployment_tags = {
        "dbx_action_type": "launch",
        "dbx_status": dbx_status,
        "dbx_environment": environment_name,
    }

    if branch_name:
        deployment_tags["dbx_branch_name"] = branch_name

    deployment_tags.update(additional_tags)
    mlflow.set_tags(deployment_tags)
