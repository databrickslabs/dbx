import json
from typing import Optional, Union, Tuple, Dict, Any

from databricks_cli.sdk import ApiClient, JobsService

from dbx.api.launch.functions import cancel_run, load_dbx_file, wait_run
from dbx.models.options import ExistingRunsOption
from dbx.models.parameters.run_now import RunNowV2d0ParamInfo, RunNowV2d1ParamInfo

from dbx.models.parameters.run_submit import RunSubmitV2d0ParamInfo, RunSubmitV2d1ParamInfo
from dbx.utils import dbx_echo
from dbx.utils.job_listing import find_job_by_name


class RunSubmitLauncher:
    def __init__(
        self, job: str, api_client: ApiClient, deployment_run_id: str, environment: str, parameters: Optional[str]
    ):
        self.run_id = deployment_run_id
        self.job = job
        self.api_client = api_client
        self.environment = environment
        self._parameters = None if not parameters else self._process_parameters(parameters)

    def _process_parameters(self, payload: str) -> Union[RunSubmitV2d0ParamInfo, RunSubmitV2d1ParamInfo]:
        _payload = json.loads(payload)

        if self.api_client.jobs_api_version == "2.1":
            return RunSubmitV2d1ParamInfo(**_payload)
        else:
            return RunSubmitV2d0ParamInfo(**_payload)

    def launch(self) -> Tuple[Dict[Any, Any], Optional[str]]:
        dbx_echo("Launching job via run submit API")

        env_spec = load_dbx_file(self.run_id, "deployment-result.json").get(self.environment)

        if not env_spec:
            raise Exception(f"No job definitions found for environment {self.environment}")

        job_specs = env_spec.get("jobs")

        found_jobs = [j for j in job_specs if j["name"] == self.job]

        if not found_jobs:
            raise Exception(f"Job definition {self.job} not found in deployment spec")

        job_spec: Dict[str, Any] = found_jobs[0]
        job_spec.pop("name")

        service = JobsService(self.api_client)

        run_data = service.submit_run(**job_spec)
        return run_data, None


class RunNowLauncher:
    def __init__(
        self, job: str, api_client: ApiClient, existing_runs: ExistingRunsOption, parameters: Optional[str] = None
    ):
        self.job = job
        self.api_client = api_client
        self.existing_runs: ExistingRunsOption = existing_runs
        self._parameters = None if not parameters else self._process_parameters(parameters)

    def _process_parameters(self, payload: str) -> Union[RunNowV2d0ParamInfo, RunNowV2d1ParamInfo]:
        _payload = json.loads(payload)
        if self.api_client.jobs_api_version == "2.1":
            return RunNowV2d1ParamInfo(**_payload)
        else:
            return RunNowV2d0ParamInfo(**_payload)

    def launch(self) -> Tuple[Dict[Any, Any], Optional[str]]:
        dbx_echo("Launching job via run now API")
        jobs_service = JobsService(self.api_client)
        job_data = find_job_by_name(jobs_service, self.job)

        if not job_data:
            raise Exception(f"Job with name {self.job} not found")

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
            dbx_echo(f"Running the workload with the provided parameters {self._parameters.dict()}")
            _additional_parameters = self._parameters.dict()
        else:
            _additional_parameters = {}

        run_data = jobs_service.run_now(job_id=job_id, **_additional_parameters)

        return run_data, job_id
