import json
from typing import List, Dict, Any, Optional

from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk import JobsService
from requests import HTTPError

from dbx.api.clients.databricks_api import DatabricksClientProvider
from dbx.models.deployment import WorkloadDefinition
from dbx.utils import dbx_echo


class AdvancedJobsService:
    def __init__(self):
        self._api_client = DatabricksClientProvider.get_v2_client()
        self._js_service = JobsService(self._api_client)
        self._js_api = JobsApi(self._api_client)

    def _list_all_jobs(self) -> List[Dict[str, Any]]:
        all_jobs = self._js_service.list_jobs(
            version="2.0"
        )  # version 2.0 is expected to list all jobs without iterations over limit/offset
        return all_jobs.get("jobs", [])

    def _find_job_by_name(self, job_name: str) -> Optional[Dict[str, Any]]:
        all_jobs = self._list_all_jobs()
        matching_jobs = [j for j in all_jobs if j["settings"]["name"] == job_name]

        if len(matching_jobs) > 1:
            raise Exception(
                f"""There are more than one jobs with name {job_name}.
                    Please delete duplicated jobs first"""
            )

        if not matching_jobs:
            return None
        else:
            return matching_jobs[0]

    def create_jobs_and_get_id_mapping(self, workloads: List[WorkloadDefinition]) -> Dict[str, int]:
        name_to_id_mapping = {}
        for workload in workloads:
            dbx_echo(f"Deploying workload {workload.name} as a job")
            matching_job = self._find_job_by_name(workload.name)

            if not matching_job:
                job_id = self._create_job(workload)
            else:
                job_id = matching_job["job_id"]
                self._update_job(job_id, workload)

            name_to_id_mapping[workload.name] = job_id
        return name_to_id_mapping

    def _create_job(self, workload: WorkloadDefinition) -> str:
        dbx_echo(f"Creating a new job with name {workload.name}")
        try:
            job_id = self._js_api.create_job(workload.to_api_format())["job_id"]
        except HTTPError as e:
            dbx_echo("Failed to create job with definition:")
            dbx_echo(json.dumps(workload.to_api_format(), indent=4))
            raise e
        return job_id

    def _update_job(self, job_id: str, workload: WorkloadDefinition) -> str:
        dbx_echo(f"Updating existing job with id: {job_id} and name: {workload.name}")
        try:
            self._js_service.reset_job(job_id, workload.to_api_format())
        except HTTPError as e:
            dbx_echo("Failed to update job with definition:")
            dbx_echo(json.dumps(workload.to_api_format(), indent=4))
            raise e
        return job_id
