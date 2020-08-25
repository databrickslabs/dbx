import json
import pathlib
import tempfile
from typing import Dict, Any, Union
from typing import List

import _jsonnet
import click
import mlflow
from databricks_cli.configure.config import debug_option
from databricks_cli.jobs.api import JobsService
from databricks_cli.sdk.api_client import ApiClient

from dbx.cli.utils import _parse_params, dbx_echo, _provide_environment, _adjust_context


@click.command(context_settings=_adjust_context(),
               short_help="""Deploys project to artifact storage with given tags.""")
@click.option("--environment", required=True, type=str, help="Environment name")
@click.option("--deployment-file", required=False, type=str,
              help="Path to deployment file. File should have .jsonnet extension.", default=".dbx/deployment.jsonnet")
@click.option("--jobs", required=False, type=str,
              help="""Comma-separated list of job names to be deployed. 
              If not provided, all jobs from deployment file will be deployed.
              """)
@click.argument('tags', nargs=-1, type=click.UNPROCESSED)
@debug_option
def deploy(environment: str, deployment_file: str, jobs: str, tags: List[str]):
    deployment_tags = _parse_params(tags)
    dbx_echo("Starting new deployment for environment %s" % environment)

    _, api_client = _provide_environment(environment)
    _verify_deployment_file(deployment_file)

    deployment = json.loads(_jsonnet.evaluate_file(deployment_file))

    if jobs:
        requested_jobs = jobs.split(",")
    else:
        requested_jobs = None

    _preprocess_deployment(deployment, requested_jobs)

    with mlflow.start_run() as deployment_run:
        _upload_files(deployment["dbfs"])

        artifact_base_uri = deployment_run.info.artifact_uri

        _adjust_job_definitions(deployment["jobs"], artifact_base_uri)
        deployment_data = _create_jobs(deployment["jobs"], api_client)
        _log_deployments(deployment_data)

        deployment_tags.update({
            "dbx_action_type": "deploy",
            "dbx_environment": environment,
            "dbx_status": "SUCCESS",
        })

        mlflow.set_tags(deployment_tags)


def _log_deployments(deployment_data):
    temp_dir = tempfile.mkdtemp()
    serialized_data = json.dumps(deployment_data, indent=4)
    temp_path = pathlib.Path(temp_dir, "deployments.json")
    temp_path.write_text(serialized_data)
    mlflow.log_artifact(str(temp_path), ".dbx")


def _verify_deployment_file(deployment_file: str):
    if not deployment_file.endswith(".jsonnet"):
        raise Exception("Deployment file should have .jsonnet extension")

    if not pathlib.Path(deployment_file).exists():
        raise Exception("Deployment file %s is non-existent: %s" % deployment_file)


def _preprocess_deployment(deployment: Dict[str, Any], requested_jobs: Union[List[str], None]):
    if "dbfs" not in deployment:
        raise Exception("No local files provided for deployment")

    _preprocess_files(deployment["dbfs"])

    if "jobs" not in deployment:
        raise Exception("No jobs provided for deployment")

    deployment["jobs"] = _preprocess_jobs(deployment["jobs"], requested_jobs)


def _preprocess_files(files: Dict[str, Any]):
    for key, file_path_str in files.items():
        file_path = pathlib.Path(file_path_str)
        if not file_path.exists():
            raise Exception("File path %s is non-existent" % file_path)
        files[key] = file_path


def _preprocess_jobs(jobs: List[Dict[str, Any]], requested_jobs: Union[List[str], None]) -> List[Dict[str, Any]]:
    job_names = [job["name"] for job in jobs]
    if requested_jobs:
        dbx_echo("Deployment will be performed only for the following jobs: %s" % requested_jobs)
        for requested_job_name in requested_jobs:
            if requested_job_name not in job_names:
                raise Exception("Job %s was requested, but not provided in deployment file" % requested_job_name)
        preprocessed_jobs = [job for job in jobs if job["name"] in requested_jobs]
    else:
        preprocessed_jobs = jobs
    return preprocessed_jobs


def _upload_files(files: Dict[str, Any]):
    for _, file_path_str in files.items():
        file_path = pathlib.Path(file_path_str)
        dbx_echo("Deploying file: %s" % file_path)
        mlflow.log_artifact(str(file_path), str(file_path.parent))


def _adjust_job_definitions(jobs: List[Dict[str, Any]], artifact_base_uri: str):
    adjustment_callback = lambda p: _adjust_path(p, artifact_base_uri)
    for job in jobs:
        _walk_content(adjustment_callback, job)


def _create_jobs(jobs: List[Dict[str, Any]], api_client: ApiClient) -> Dict[str, int]:
    deployment_data = {}
    for job in jobs:
        dbx_echo("Processing deployment for job: %s" % job["name"])
        jobs_service = JobsService(api_client)
        all_jobs = jobs_service.list_jobs()["jobs"]
        matching_jobs = [j for j in all_jobs if j["settings"]["name"] == job["name"]]

        if not matching_jobs:
            dbx_echo("Creating a new job")
            job_id = api_client.perform_query('POST', '/jobs/create', data=job, headers=None)["job_id"]
        else:
            job_id = matching_jobs[0]["job_id"]
            dbx_echo("Updating existing job with id: %s" % job_id)
            jobs_service.reset_job(job_id, job)

        deployment_data[job["name"]] = job_id
    return deployment_data


def _walk_content(func, content, parent=None, index=None):
    if isinstance(content, dict):
        for key, item in content.items():
            _walk_content(func, item, content, key)
    elif isinstance(content, list):
        for idx, sub_item in enumerate(content):
            _walk_content(func, sub_item, content, idx)
    else:
        parent[index] = func(content)


def _adjust_path(candidate, adjustment):
    if isinstance(candidate, str):
        if pathlib.Path(candidate).exists():
            adjusted_path = "%s/%s" % (adjustment, candidate)
            dbx_echo("Adjusting path %s => %s" % (candidate, adjusted_path))
            return adjusted_path
        else:
            return candidate
    else:
        return candidate
