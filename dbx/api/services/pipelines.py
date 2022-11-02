from typing import Optional, List

from requests import HTTPError
from rich.markup import escape

from dbx.api.services._base import WorkflowBaseService
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.pipeline import Pipeline
from dbx.utils import dbx_echo


class NamedPipelinesService(WorkflowBaseService):
    def delete(self, object_id: str):
        self.api_client.perform_query("DELETE", path=f"/pipelines/{object_id}")

    def find_by_name_strict(self, name: str):
        _response = ListPipelinesResponse(
            **self.api_client.perform_query(method="GET", path="/pipelines/", data={"filter": f"name like '{name}'"})
        )
        _found = _response.get(name, strict_exist=True)
        return _found.pipeline_id

    def find_by_name(self, name: str) -> Optional[str]:
        _response = ListPipelinesResponse(
            **self.api_client.perform_query(method="GET", path="/pipelines/", data={"filter": f"name like '{name}'"})
        )
        _found = _response.get(name, strict_exist=False)
        return _found.pipeline_id if _found else None

    def create(self, wf: Pipeline):
        dbx_echo(f"🪄  Creating new DLT pipeline with name {escape(wf.name)}")
        payload = wf.dict(exclude_none=True)
        try:
            _response = self.api_client.perform_query("POST", path="/pipelines", data=payload)
            wf.pipeline_id = _response["pipeline_id"]
        except HTTPError as e:
            dbx_echo(":boom: Failed to create pipeline with definition:")
            dbx_echo(payload)
            raise e

    def update(self, object_id: int, wf: Pipeline):
        dbx_echo(f"🪄  Updating existing DLT pipeline with name [yellow]{escape(wf.name)}[/yellow] and id: {object_id}")
        payload = wf.dict(exclude_none=True)
        try:
            self.api_client.perform_query("PUT", path=f"/pipelines/{object_id}", data=payload)
            wf.pipeline_id = object_id
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

    def get(self, name: str, strict_exist: bool = True) -> Optional[PipelineStateInfo]:
        _found = list(filter(lambda p: p.name == name, self.statuses))

        if strict_exist:
            assert _found, NameError(f"No pipelines with name {name} were found!")

        if not _found:
            return None
        else:
            assert len(_found) == 1, NameError(f"More than one pipeline with name {name} was found: {_found}")
            return _found[0]
