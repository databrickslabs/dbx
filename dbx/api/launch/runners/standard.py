import json
from typing import Optional, Union, Tuple, Dict, Any

from databricks_cli.sdk import ApiClient, JobsService

from dbx.api.launch.functions import wait_run, cancel_run
from dbx.models.cli.options import ExistingRunsOption
from dbx.utils import dbx_echo
from dbx.utils.job_listing import find_job_by_name
from dbx.models.workflow.v2dot0.parameters import StandardRunPayload as V2dot0StandardRunPayload
from dbx.models.workflow.v2dot1.parameters import StandardRunPayload as V2dot1StandardRunPayload


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

    def launch(self) -> Tuple[Dict[Any, Any], Optional[str]]:
        dbx_echo("Launching job via run now API")
        jobs_service = JobsService(self.api_client)
        job_data = find_job_by_name(jobs_service, self.workflow_name)

        if not job_data:
            raise Exception(f"Job with name {self.workflow_name} not found")

        job_id = job_data["job_id"]

        active_runs = jobs_service.list_runs(job_id, active_only=True).get("runs", [])

        for run in active_runs:
            if self.existing_runs == ExistingRunsOption.pass_:
                dbx_echo("Passing the existing runs status check")
            elif self.existing_runs == ExistingRunsOption.wait:
                dbx_echo(f'Waiting for job run with id {run["run_id"]} to be finished')
                wait_run(self.api_client, run)
            elif self.existing_runs == ExistingRunsOption.cancel:
                dbx_echo(f'Cancelling run with id {run["run_id"]}')
                cancel_run(self.api_client, run)

        if self._parameters:
            dbx_echo(f"Running the workload with the provided parameters {self._parameters.dict(exclude_none=True)}")
            _additional_parameters = self._parameters.dict()
        else:
            _additional_parameters = {}

        run_data = jobs_service.run_now(job_id=job_id, **_additional_parameters)

        return run_data, job_id
