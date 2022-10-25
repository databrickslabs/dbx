import functools
from typing import Optional, Any, List

from dbx.api.adjuster.mixins.base import ApiClientMixin, ElementSetterMixin
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.new_cluster import NewCluster


class InstanceProfileInfo(FlexibleModel):
    instance_profile_arn: str

    @property
    def instance_profile_name(self):
        return self.instance_profile_arn.split("/")[-1]


class ListInstanceProfilesResponse(FlexibleModel):
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
    def _adjust_legacy_instance_profile_ref(self, element: NewCluster):
        _arn = self._instance_profiles.get(element.aws_attributes.instance_profile_name).instance_profile_arn
        element.aws_attributes.instance_profile_arn = _arn

    def _adjust_instance_profile_ref(self, element: str, parent: Any, index: Any):
        _arn = self._instance_profiles.get(element.replace("instance-profile://", "")).instance_profile_arn
        self.set_element_at_parent(_arn, parent, index)

    @functools.cached_property
    def _instance_profiles(self) -> ListInstanceProfilesResponse:
        return ListInstanceProfilesResponse(
            **self.api_client.perform_query(method="GET", path="/instance-profiles/list")
        )
