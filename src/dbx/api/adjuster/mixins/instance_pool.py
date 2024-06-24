import functools
from typing import Any, List, Optional

from databricks_cli.sdk import InstancePoolService

from dbx.api.adjuster.mixins.base import ApiClientMixin, ElementSetterMixin
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.new_cluster import NewCluster


class InstancePoolInfo(FlexibleModel):
    instance_pool_name: str
    instance_pool_id: str


class ListInstancePoolsResponse(FlexibleModel):
    instance_pools: Optional[List[InstancePoolInfo]] = []

    @property
    def pool_names(self) -> List[str]:
        return [p.instance_pool_name for p in self.instance_pools]

    def get_pool(self, name: str) -> InstancePoolInfo:
        _found = list(filter(lambda p: p.instance_pool_name == name, self.instance_pools))
        assert _found, NameError(f"No pools with name {name} were found, available pools are {self.pool_names}")
        assert len(_found) == 1, NameError(f"More than one pool with name {name} was found: {_found}")
        return _found[0]


class InstancePoolAdjuster(ApiClientMixin, ElementSetterMixin):
    @functools.cached_property
    def _instance_pools(self) -> ListInstancePoolsResponse:
        _service = InstancePoolService(self.api_client)
        return ListInstancePoolsResponse(**_service.list_instance_pools())

    def _adjust_legacy_driver_instance_pool_ref(self, element: NewCluster):
        element.driver_instance_pool_id = self._instance_pools.get_pool(
            element.driver_instance_pool_name
        ).instance_pool_id

    def _adjust_legacy_instance_pool_ref(self, element: NewCluster):
        element.instance_pool_id = self._instance_pools.get_pool(element.instance_pool_name).instance_pool_id

    def _adjust_instance_pool_ref(self, element: str, parent: Any, index: Any):
        pool_id = self._instance_pools.get_pool(element.replace("instance-pool://", "")).instance_pool_id
        self.set_element_at_parent(pool_id, parent, index)
