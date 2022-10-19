from abc import ABC
from pathlib import Path
from typing import Optional, Any, Tuple

from databricks_cli.sdk import ApiClient
from pydantic import BaseModel

from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.new_cluster import NewCluster
from dbx.models.workflow.v2dot0.workflow import Workflow as V2dot0Workflow
from dbx.utils.file_uploader import AbstractFileUploader


class ApiClientMixin(ABC):
    def __init__(self, api_client: ApiClient):
        self.api_client = api_client


class ElementSetterMixin:
    @classmethod
    def set_element_at_parent(cls, element, parent, index) -> None:
        """
        Sets the element value for various types of parent
        :param element: New element value
        :param parent: A nested structure where element should be placed
        :param index: Position (or pointer) where element should be provided
        :return: None
        """
        if isinstance(parent, dict) or isinstance(parent, list):
            parent[index] = element
        elif isinstance(parent, (BaseModel, FlexibleModel)):
            parent.__setattr__(index, element)
        else:
            raise ValueError(
                "Cannot apply reference to the parent structure."
                f"Please create a GitHub issue providing the following parent object type: {type(parent)}"
            )


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


class FileReferenceAdjuster(ElementSetterMixin):
    def __init__(self, file_uploader: AbstractFileUploader, **kwargs):
        self._uploader = file_uploader

    @staticmethod
    def _preprocess_element(element: str) -> Tuple[Path, bool]:
        _as_fuse = element.startswith("file:fuse://")
        _corrected = element.replace("file:fuse://", "") if _as_fuse else element.replace("file://", "")
        _path = Path(_corrected)
        return _path, _as_fuse

    @staticmethod
    def _verify_reference(element, _path: Path):
        if not _path.exists():
            raise FileNotFoundError(f"Provided file reference: {element} doesn't exist in the local FS")

    def _adjust_file_ref(self, element: str, parent: Any, index: Any):
        _path, as_fuse = self._preprocess_element(element)
        self._verify_reference(element, _path)

        _uploaded = self._uploader.upload_and_provide_path(_path, as_fuse)
        self.set_element_at_parent(_uploaded, parent, index)


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
