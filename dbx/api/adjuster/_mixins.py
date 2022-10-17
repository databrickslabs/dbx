from abc import ABC
from typing import Optional, Any

from databricks_cli.sdk import ApiClient

from dbx.models.workflow.common.new_cluster import NewCluster
from dbx.models.workflow.v2dot0.workflow import Workflow as V2dot0Workflow


class ApiClientMixin(ABC):
    def __init__(self, api_client: ApiClient):
        self.api_client = api_client


class InstancePoolAdjuster(ApiClientMixin):
    def _adjust_legacy_driver_instance_pool_ref(self, element: NewCluster):
        pass

    def _adjust_legacy_instance_pool_ref(self, element: NewCluster):
        pass

    def _adjust_instance_pool_ref(self, element: str, parent: Any, index: Any):
        pass


class ExistingClusterAdjuster(ApiClientMixin):
    def _adjust_legacy_existing_cluster(self, element: V2dot0Workflow):
        pass


class InstanceProfileAdjuster(ApiClientMixin):
    def _adjust_legacy_instance_profile_ref(self, element: NewCluster):
        pass

    def _adjust_instance_profile_ref(self, element: str, parent: Optional[Any] = None, index: Optional[Any] = None):
        pass


class FileReferenceAdjuster(ApiClientMixin):
    def _adjust_file_ref(self, element: str, parent: Any, index: Any):
        pass


class PipelineAdjuster(ApiClientMixin):
    def _adjust_pipeline_ref(self, element: str, parent: Any, index: Any):
        pass


class ServicePrincipalAdjuster(ApiClientMixin):
    def _adjust_service_principal_ref(self, element: str, parent: Any, index: Any):
        pass


class WarehouseAdjuster(ApiClientMixin):
    def _adjust_warehouse_ref(self, element: str, parent: Any, index: Any):
        pass


class QueryAdjuster(ApiClientMixin):
    def _adjust_query_ref(self, element: str, parent: Any, index: Any):
        pass


class DashboardAdjuster(ApiClientMixin):
    def _adjust_dashboard_ref(self, element: str, parent: Any, index: Any):
        pass


class AlertAdjuster(ApiClientMixin):
    def _adjust_alert_ref(self, element: str, parent: Any, index: Any):
        pass
