import time
from typing import List, Optional, Union

from databricks_cli.sdk import ApiClient, JobsService
from requests import HTTPError
from rich.console import Console
from rich.markup import escape

from dbx.api.services._base import WorkflowBaseService
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.v2dot0.workflow import Workflow as V2dot0Workflow
from dbx.models.workflow.v2dot1.workflow import Workflow as V2dot1Workflow
from dbx.utils import dbx_echo

AnyJob = Union[V2dot0Workflow, V2dot1Workflow]


class JobSettingsResponse(FlexibleModel):
    name: str


class JobResponse(FlexibleModel):
    job_id: int
    settings: JobSettingsResponse


class ListJobsResponse(FlexibleModel):
    has_more: Optional[bool] = False
    jobs: Optional[List[JobResponse]] = []


class NamedJobsService(WorkflowBaseService):
    DEFAULT_LIST_LIMIT = 25
    JOBS_API_VERSION_FOR_SEARCH = "2.1"

    def __init__(self, api_client: ApiClient):
        super().__init__(api_client)
        self._service = JobsService(api_client)

    def find_by_name(self, name: str) -> Optional[int]:
        offset = 0
        all_jobs = []

        with Console().status(f"Searching for a job with name {name}", spinner="dots"):
            while True:
                time.sleep(0.5)  # to avoid bursting out the jobs API
                _response = ListJobsResponse(
                    **self._service.list_jobs(
                        limit=self.DEFAULT_LIST_LIMIT, offset=offset, version=self.JOBS_API_VERSION_FOR_SEARCH
                    )
                )
                all_jobs.extend(_response.jobs)
                offset += self.DEFAULT_LIST_LIMIT
                if not _response.has_more:
                    break

        _found_ids = [j.job_id for j in all_jobs if j.settings.name == name]

        if len(_found_ids) > 1:
            raise Exception(
                f"""There are more than one jobs with name {name}.
                    Please delete duplicated jobs first."""
            )

        if not _found_ids:
            return None
        else:
            return _found_ids[0]

    def create(self, wf: AnyJob):
        """
        Please note that this method adjusts the provided workflow definition
        by setting the job_id field value on it
        """
        dbx_echo(f"🪄  Creating new workflow with name {escape(wf.name)}")
        payload = wf.dict(exclude_none=True)
        try:
            _response = self.api_client.perform_query("POST", "/jobs/create", data=payload)
            wf.job_id = _response["job_id"]
        except HTTPError as e:
            dbx_echo(":boom: Failed to create job with definition:")
            dbx_echo(payload)
            raise e

    def update(self, object_id: int, wf: AnyJob):
        dbx_echo(f"🪄  Updating existing workflow with name {escape(wf.name)} and id: {object_id}")
        payload = wf.dict(exclude_none=True)
        wf.job_id = object_id
        try:
            self._service.reset_job(object_id, payload)
        except HTTPError as e:
            dbx_echo(":boom: Failed to update job with definition:")
            dbx_echo(payload)
            raise e

    def delete(self, object_id: int):
        self._service.delete_job(object_id)
