import functools
from typing import Any, List

from databricks_cli.sdk import DeltaPipelinesService

from dbx.api.adjuster.mixins.base import ApiClientMixin, ElementSetterMixin
from dbx.models.workflow.common.flexible import FlexibleModel


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


class PipelineAdjuster(ApiClientMixin, ElementSetterMixin):
    @functools.cached_property
    def _pipelines(self) -> ListPipelinesResponse:
        # TODO: add paginated calls
        _service = DeltaPipelinesService(self.api_client)
        return ListPipelinesResponse(**_service.list())

    def _adjust_pipeline_ref(self, element: str, parent: Any, index: Any):
        _pipeline_id = self._pipelines.get(element.replace("pipeline://", "")).pipeline_id
        self.set_element_at_parent(_pipeline_id, parent, index)
