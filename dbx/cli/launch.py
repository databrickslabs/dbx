from copy import deepcopy

import click
import mlflow
import time
from databricks_cli.configure.provider import ProfileConfigProvider
from databricks_cli.jobs.api import JobsService
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.cli.utils import dbx_echo, parse_params, InfoFile, generate_filter_string, \
    prepare_job_config, DATABRICKS_MLFLOW_URI

"""
Logic behind this functionality:
0. List all mlflow runs per experiment
1. Select latest run by given tags
2. Start subrun within the given run
3. Create job spec by given configurations
4. Launch the job
"""

adopted_context = deepcopy(CONTEXT_SETTINGS)

adopted_context.update(dict(
    ignore_unknown_options=True,
))


@click.command(context_settings=adopted_context,
               short_help="Launches job, choosing the latest version by given tags")
@click.option("--environment", required=True, type=str, help="Environment name")
@click.option("--entrypoint-file", required=True, type=str, help="Entrypoint file location in artifactory")
@click.option("--job-conf-file", required=True, type=str, help="Job configuration path in artifactory")
@click.option("--trace", is_flag=True, help="Trace the job until it finishes")
@click.argument('tags', nargs=-1, type=click.UNPROCESSED)
def launch(environment, entrypoint_file, job_conf_file, trace, tags):
    """
    Launches job, choosing the latest version by given tags. Please provide tags in format: --tag1=value1 --tag2=value2
    """
    deployment_tags = parse_params(tags)
    dbx_echo("Launching job by given parameters")

    environment_data = InfoFile.get("environments").get(environment)

    profile_config = ProfileConfigProvider(environment_data["profile"]).get_config()

    api_client = ApiClient(host=profile_config.host, token=profile_config.token)

    mlflow.set_tracking_uri("%s://%s" % (DATABRICKS_MLFLOW_URI, environment_data["profile"]))
    mlflow.set_experiment(environment_data["workspace_dir"])

    filter_string = generate_filter_string(environment, deployment_tags)

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
            job_conf_file_path = "%s/%s" % (artifact_base_uri, job_conf_file)
            entrypoint_file_path = "%s/%s" % (artifact_base_uri, entrypoint_file)

            job_conf = prepare_job_config(
                api_client,
                job_conf_file_path,
                entrypoint_file_path
            )

            job_id = api_client.perform_query('POST', '/jobs/create', data=job_conf, headers=None)["job_id"]

            jobs_service = JobsService(api_client)
            run_data = jobs_service.run_now(job_id)

            if trace:
                dbx_status = trace_run(api_client, run_data)
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


def trace_run(api_client, run_data):
    dbx_echo("Tracing job run")
    while True:
        status = get_run_status(api_client, run_data)
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


def get_run_status(api_client, run_data):
    run_status = api_client.perform_query('GET', '/jobs/runs/get', data={"run_id": run_data["run_id"]}, headers=None)
    return run_status
