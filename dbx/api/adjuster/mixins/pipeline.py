from typing import Any

from dbx.api.adjuster.mixins.base import ApiClientMixin, ElementSetterMixin
from dbx.api.services.pipelines import NamedPipelinesService


class PipelineAdjuster(ApiClientMixin, ElementSetterMixin):
    def _adjust_pipeline_ref(self, element: str, parent: Any, index: Any):
        _pipeline_id = NamedPipelinesService(self.api_client).find_by_name_strict(element.replace("pipeline://", ""))
        self.set_element_at_parent(_pipeline_id, parent, index)
