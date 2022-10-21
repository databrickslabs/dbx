import functools
from typing import Optional, Any, List

from databricks_cli.sdk import ApiClient

from dbx.api.adjuster.mixins.base import ApiClientMixin, ElementSetterMixin
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.new_cluster import NewCluster
from dbx.utils import dbx_echo


class InstanceProfileInfo(FlexibleModel):
    instance_profile_arn: str

    @property
    def instance_profile_name(self):
        return self.instance_profile_arn.split("/")[-1]


class ListInstancePoolsResponse(FlexibleModel):
    instance_profiles: Optional[List[InstanceProfileInfo]] = []

    @property
    def names(self) -> List[str]:
        return [p.instance_profile_name for p in self.instance_profiles]

    def get(self, name: str) -> InstanceProfileInfo:
        _found = list(filter(lambda p: p.instance_profile_name == name, self.instance_profiles))
        assert _found, NameError(
            f"No instance profiles with name {name} were found, available instance profiles are {self.names}"
        )
        assert len(_found) == 1, NameError(f"More than one instance profile with name {name} was found: {_found}")
        return _found[0]


class InstanceProfileAdjuster(ApiClientMixin, ElementSetterMixin):
    def __init__(self, api_client: ApiClient):
        super().__init__(api_client)

    def _adjust_legacy_instance_profile_ref(self, element: NewCluster):
        dbx_echo("⏳ Processing the instance_profile_name reference")
        _arn = self._instance_profiles.get(element.aws_attributes.instance_profile_name).instance_profile_arn
        element.aws_attributes.instance_profile_arn = _arn
        dbx_echo("✅ Processing the instance_profile_name reference - done")

    def _adjust_instance_profile_ref(self, element: str, parent: Any, index: Any):
        dbx_echo(f"⏳ Processing reference {element}")
        _arn = self._instance_profiles.get(element.replace("instance-profile://", "")).instance_profile_arn
        self.set_element_at_parent(_arn, parent, index)
        dbx_echo(f"✅ Processing reference {element} - done")

    @functools.cached_property
    def _instance_profiles(self) -> ListInstancePoolsResponse:
        return ListInstancePoolsResponse(**self.api_client.perform_query(method="GET", path="/instance-profiles/list"))
