from typing import Optional, List

from requests import HTTPError
from rich.markup import escape

from dbx.api.services._base import WorkflowBaseService
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.pipeline import Pipeline
from dbx.utils import dbx_echo


class NamedPipelinesService(WorkflowBaseService):
    def find_by_name(self, name: str) -> Optional[str]:
        _response = ListPipelinesResponse(
            **self.api_client.perform_query(method="GET", path="/pipelines/", data={"filter": f"name like '{name}'"})
        )
        return _response.get(name).pipeline_id

    def create(self, wf: Pipeline):
        dbx_echo(f"Creating a new workflow with name {escape(wf.name)} in format of {wf.workflow_type}")
        payload = wf.dict(exclude_none=True)
        try:
            _response = self.api_client.perform_query("POST", path="/pipelines", data=payload)
            wf.pipeline_id = _response["pipeline_id"]
        except HTTPError as e:
            dbx_echo(":boom: Failed to create pipeline with definition:")
            dbx_echo(payload)
            raise e

    def update(self, object_id: int, wf: Pipeline):
        payload = wf.dict(exclude_none=True)
        try:
            self.api_client.perform_query("PUT", path=f"/pipelines/{wf.pipeline_id}", data=payload)
        except HTTPError as e:
            dbx_echo(":boom: Failed to edit pipeline with definition:")
            dbx_echo(payload)
            raise e


class PipelineStateInfo(FlexibleModel):
    pipeline_id: str
    name: str


class ListPipelinesResponse(FlexibleModel):
    statuses: List[PipelineStateInfo] = []

    @property
    def pipeline_names(self) -> List[str]:
        return [p.name for p in self.statuses]

    def get(self, name: str) -> PipelineStateInfo:
        _found = list(filter(lambda p: p.name == name, self.statuses))
        assert _found, NameError(
            f"No pipelines with name {name} were found, available pipelines are {self.pipeline_names}"
        )
        assert len(_found) == 1, NameError(f"More than one pipeline with name {name} was found: {_found}")
        return _found[0]
