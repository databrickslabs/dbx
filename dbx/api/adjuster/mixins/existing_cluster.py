import functools
from typing import Optional, List, Any

from databricks_cli.sdk import ClusterService

from dbx.api.adjuster.mixins.base import ApiClientMixin, ElementSetterMixin
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.v2dot0.workflow import Workflow as V2dot0Workflow


class ClusterInfo(FlexibleModel):
    cluster_id: str
    cluster_name: str


class ListClustersResponse(FlexibleModel):
    clusters: Optional[List[ClusterInfo]] = []

    @property
    def cluster_names(self) -> List[str]:
        return [p.cluster_name for p in self.clusters]

    def get_cluster(self, name: str) -> ClusterInfo:
        _found = list(filter(lambda p: p.cluster_name == name, self.clusters))
        assert _found, NameError(
            f"No clusters with name {name} were found. Available clusters are {self.cluster_names}"
        )
        assert len(_found) == 1, NameError(f"More than one cluster with name {name} was found: {_found}")
        return _found[0]


class ExistingClusterAdjuster(ApiClientMixin, ElementSetterMixin):
    def _adjust_legacy_existing_cluster(self, element: V2dot0Workflow):
        element.existing_cluster_id = self._clusters.get_cluster(element.existing_cluster_name).cluster_id

    def _adjust_existing_cluster_ref(self, element: str, parent: Any, index: Any):
        _id = self._clusters.get_cluster(element.replace("cluster://", "")).cluster_id
        self.set_element_at_parent(_id, parent, index)

    @functools.cached_property
    def _clusters(self) -> ListClustersResponse:
        _service = ClusterService(self.api_client)
        return ListClustersResponse(**_service.list_clusters())
