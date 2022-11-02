import json
from typing import Optional, Union, Tuple

from databricks_cli.sdk import ApiClient, JobsService

from dbx.api.launch.functions import wait_run, cancel_run
from dbx.api.launch.runners.base import RunData
from dbx.api.services.jobs import NamedJobsService
from dbx.models.cli.options import ExistingRunsOption
from dbx.models.workflow.v2dot0.parameters import StandardRunPayload as V2dot0StandardRunPayload
from dbx.models.workflow.v2dot1.parameters import StandardRunPayload as V2dot1StandardRunPayload
from dbx.utils import dbx_echo


class StandardLauncher:
    def __init__(
        self,
        workflow_name: str,
        api_client: ApiClient,
        existing_runs: ExistingRunsOption,
        parameters: Optional[str] = None,
    ):
        self.workflow_name = workflow_name
        self.api_client = api_client
        self.existing_runs: ExistingRunsOption = existing_runs
        self._parameters = None if not parameters else self._process_parameters(parameters)

    def _process_parameters(self, payload: str) -> Union[V2dot0StandardRunPayload, V2dot1StandardRunPayload]:
        _payload = json.loads(payload)
        if self.api_client.jobs_api_version == "2.0":
            return V2dot0StandardRunPayload(**_payload)
        else:
            return V2dot1StandardRunPayload(**_payload)

    def launch(self) -> Tuple[RunData, int]:
        dbx_echo("Launching job via run now API")
        named_service = NamedJobsService(self.api_client)
        standard_service = JobsService(self.api_client)
        job_id = named_service.find_by_name(self.workflow_name)

        if not job_id:
            raise Exception(f"Workflow with name {self.workflow_name} not found")

        active_runs = standard_service.list_runs(job_id, active_only=True).get("runs", [])

        for run in active_runs:
            _run_data = RunData(**run)
            if self.existing_runs == ExistingRunsOption.wait:
                dbx_echo(f"Waiting for job run with id {_run_data.run_id} to be finished")
                wait_run(self.api_client, _run_data)
            elif self.existing_runs == ExistingRunsOption.cancel:
                dbx_echo(f"Cancelling run with id {_run_data.run_id}")
                cancel_run(self.api_client, _run_data)
            else:
                dbx_echo("Passing the existing runs status check")

        api_request_payload = {"job_id": job_id}

        if self._parameters:
            dbx_echo(f"Running the workload with the provided parameters {self._parameters.dict(exclude_none=True)}")
            api_request_payload.update(self._parameters.dict(exclude_none=True))

        run_data = self.api_client.perform_query("POST", "/jobs/run-now", data=api_request_payload)

        return RunData(**run_data), job_id
