import functools
from typing import Any, List

from pydantic import Field

from dbx.api.adjuster.mixins.base import ApiClientMixin, ElementSetterMixin
from dbx.models.workflow.common.flexible import FlexibleModel


class ResourceInfo(FlexibleModel):
    display_name: str = Field(str, alias="displayName")  # noqa
    application_id: str = Field(str, alias="applicationId")  # noqa


class ListServicePrincipals(FlexibleModel):
    Resources: List[ResourceInfo]

    @property
    def names(self) -> List[str]:
        return [p.display_name for p in self.Resources]

    def get(self, name: str) -> ResourceInfo:
        _found = list(filter(lambda p: p.display_name == name, self.Resources))
        assert _found, NameError(
            f"No service principals with name {name} were found, available objects are {self.names}"
        )
        assert len(_found) == 1, NameError(f"More than one service principal with name {name} was found: {_found}")
        return _found[0]


class ServicePrincipalAdjuster(ApiClientMixin, ElementSetterMixin):
    def _adjust_service_principal_ref(self, element: str, parent: Any, index: Any):
        app_id = self._principals.get(element.replace("service-principal://", "")).application_id
        self.set_element_at_parent(app_id, parent, index)

    @functools.cached_property
    def _principals(self) -> ListServicePrincipals:
        return ListServicePrincipals(**self.api_client.perform_query("GET", path="/preview/scim/v2/ServicePrincipals"))
