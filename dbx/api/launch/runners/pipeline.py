import json
import time
from functools import partial
from typing import Optional, List, Tuple

from databricks_cli.sdk import ApiClient
from pydantic import BaseModel
from rich.console import Console

from dbx.api.launch.pipeline_models import PipelineDetails, PipelineGlobalState
from dbx.api.launch.runners.base import PipelineUpdateResponse
from dbx.api.services.pipelines import NamedPipelinesService


class PipelinesRunPayload(BaseModel):
    full_refresh: Optional[bool]
    refresh_selection: Optional[List[str]] = []
    full_refresh_selection: Optional[List[str]] = []


class PipelineLauncher:
    def __init__(
        self,
        workflow_name: str,
        api_client: ApiClient,
        parameters: Optional[str] = None,
    ):
        self.api_client = api_client
        self.name = workflow_name
        self.parameters = self._process_parameters(parameters)

    @staticmethod
    def _process_parameters(payload: Optional[str]) -> Optional[PipelinesRunPayload]:
        if payload:
            _payload = json.loads(payload)
            return PipelinesRunPayload(**_payload)

    def _stop_current_updates(self, pipeline_id: str):
        msg = f"Checking if there are any running updates for the pipeline {pipeline_id} and stopping them."
        with Console().status(msg, spinner="dots") as status:
            while True:
                raw_response = self.api_client.perform_query("GET", f"/pipelines/{pipeline_id}")
                current_pipeline_status = PipelineDetails(**raw_response)
                if current_pipeline_status.state == PipelineGlobalState.RUNNING:
                    status.update("Found a running pipeline update, stopping it")
                    self.api_client.perform_query("POST", f"/pipelines/{pipeline_id}/stop")
                    time.sleep(5)
                else:
                    status.update(f"Pipeline {pipeline_id} is stopped, starting a new update")
                    break

    def launch(self) -> Tuple[PipelineUpdateResponse, str]:
        pipeline_id = NamedPipelinesService(self.api_client).find_by_name_strict(self.name)
        self._stop_current_updates(pipeline_id)
        prepared_query = partial(self.api_client.perform_query, "POST", f"/pipelines/{pipeline_id}/updates")
        resp = prepared_query(data=self.parameters.dict(exclude_none=True)) if self.parameters else prepared_query()
        return PipelineUpdateResponse(**resp), pipeline_id
