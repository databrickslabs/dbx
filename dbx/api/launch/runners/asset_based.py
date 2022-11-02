import inspect
import json
from typing import Optional, Union, Tuple, Dict, Any

from databricks_cli.sdk import ApiClient, JobsService

from dbx.api.configure import ProjectConfigurationManager
from dbx.api.launch.processors import ClusterReusePreprocessor
from dbx.api.launch.runners.base import RunData
from dbx.api.storage.io import StorageIO
from dbx.models.deployment import EnvironmentDeploymentInfo
from dbx.models.workflow.v2dot0.parameters import AssetBasedRunPayload as V2dot0AssetBasedParametersPayload
from dbx.models.workflow.v2dot1.parameters import AssetBasedRunPayload as V2dot1AssetBasedParametersPayload
from dbx.models.workflow.v2dot1.workflow import Workflow as V2dot1Workflow
from dbx.utils import dbx_echo


class AssetBasedLauncher:
    def __init__(
        self,
        workflow_name: str,
        api_client: ApiClient,
        deployment_run_id: str,
        environment_name: str,
        parameters: Optional[str] = None,
    ):
        self.run_id = deployment_run_id
        self.workflow_name = workflow_name
        self.api_client = api_client
        self.environment_name = environment_name
        self.failsafe_cluster_reuse = ProjectConfigurationManager().get_failsafe_cluster_reuse()
        self._parameters = None if not parameters else self._process_parameters(parameters)

    def _process_parameters(
        self, payload: str
    ) -> Union[V2dot0AssetBasedParametersPayload, V2dot1AssetBasedParametersPayload]:

        if self.api_client.jobs_api_version == "2.0":
            return V2dot0AssetBasedParametersPayload(**json.loads(payload))
        else:
            return V2dot1AssetBasedParametersPayload.from_string(payload)

    def launch(self) -> Tuple[RunData, Optional[int]]:
        dbx_echo(
            f"Launching workflow in assets-based mode "
            f"(via RunSubmit method, Jobs API V{self.api_client.jobs_api_version})"
        )

        service = JobsService(self.api_client)
        env_spec = StorageIO.load(self.run_id, "deployment-result.json")
        _config = EnvironmentDeploymentInfo.from_spec(
            self.environment_name, env_spec.get(self.environment_name), reader_type="remote"
        )

        workflow = _config.payload.get_workflow(self.workflow_name)

        if self.failsafe_cluster_reuse:
            if isinstance(workflow, V2dot1Workflow) and workflow.job_clusters:
                ClusterReusePreprocessor.process(workflow)

        if self._parameters:
            dbx_echo(f"Running the workload with the provided parameters {self._parameters.dict(exclude_none=True)}")
            workflow.override_asset_based_launch_parameters(self._parameters)

        final_spec = workflow.dict(exclude_none=True, exclude_unset=True)
        cleaned_spec = self._cleanup_unsupported_properties(final_spec)
        run_data = service.submit_run(**cleaned_spec)

        return RunData(**run_data), None

    @staticmethod
    def _cleanup_unsupported_properties(spec: Dict[str, Any]) -> Dict[str, Any]:
        expected_props = inspect.getfullargspec(JobsService.submit_run).args
        cleaned_args = {}
        for _prop in spec:
            if _prop not in expected_props:
                dbx_echo(
                    f"[yellow bold]Property {_prop} is not supported in the assets-only launch mode."
                    f" It will be ignored during current launch.[/yellow bold]"
                )
            else:
                cleaned_args[_prop] = spec[_prop]
        return cleaned_args
