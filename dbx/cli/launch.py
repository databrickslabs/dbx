from copy import deepcopy

import click
import mlflow
import time
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.jobs.api import JobsService
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.cli.utils import dbx_echo, setup_mlflow, custom_profile_option, parse_tags, InfoFile, generate_filter_string, \
    prepare_job_config, get_dist

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
@click.option("--job-name", required=True, type=str, help="name of a job to be launched")
@click.option("--env", required=True, type=str, help="Environment name")
@click.option("--trace", is_flag=True, help="Trace the job until it finishes")
@click.argument('tags', nargs=-1, type=click.UNPROCESSED)
@debug_option
@custom_profile_option
@setup_mlflow
@profile_option
@provide_api_client
def launch(trace, env, api_client, job_name, tags):
    """
    Launches job, choosing the latest version by given tags. Please provide tags in format: --tag1=value1 --tag2=value2
    """
    deployment_tags = parse_tags(tags)
    project_name = InfoFile.get("project_name")
    dbx_echo("Launching job %s for project: %s with tags %s" % (job_name, project_name, deployment_tags))

    experiment_info = mlflow.get_experiment_by_name(InfoFile.get("experiment_path"))

    experiment_id = experiment_info.experiment_id

    filter_string = generate_filter_string(env, deployment_tags)
    runs = mlflow.search_runs(experiment_ids=[experiment_id], filter_string=filter_string, max_results=1)

    if runs.empty:
        raise EnvironmentError("""
        No runs provided per given set of filters:
            %s
        Please check filters experiment UI to verify current status of deployments.
        """ % filter_string)

    run_info = runs.iloc[0].to_dict()

    deployment_run_id = run_info["run_id"]

    with mlflow.start_run(run_id=deployment_run_id) as deploy_run:
        with mlflow.start_run(nested=True):
            artifact_base_uri = deploy_run.info.artifact_uri
            package_path = get_dist(api_client, artifact_base_uri)
            config_path = "%s/config/%s/%s/job.json" % (artifact_base_uri, env, job_name)

            entrypoint_path = "%s/%s/jobs/%s/entrypoint.py" % (
                artifact_base_uri,
                InfoFile.get("project_name"),
                job_name
            )

            job_conf = prepare_job_config(api_client, package_path, config_path, entrypoint_path)

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
                "job_name": job_name,
                "action_type": "launch",
                "dbx_status": dbx_status
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
