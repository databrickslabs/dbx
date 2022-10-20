from typing import Any, List

from databricks_cli.sdk import ApiClient

from dbx.api.adjuster.mixins.base import ApiClientMixin, ElementSetterMixin
from dbx.models.workflow.common.flexible import FlexibleModel


class NamedModel(FlexibleModel):
    id: str
    name: str


class WarehouseInfo(NamedModel):
    """"""


class QueryInfo(FlexibleModel):
    """"""


class DashboardInfo(FlexibleModel):
    """"""


class AlertInfo(FlexibleModel):
    """"""


class WarehousesList(FlexibleModel):
    warehouses: List[WarehouseInfo] = []

    @property
    def names(self) -> List[str]:
        return [p.name for p in self.warehouses]

    def get(self, name: str) -> WarehouseInfo:
        _found = list(filter(lambda p: p.name == name, self.warehouses))
        assert _found, NameError(f"No warehouses with name {name} were found, available warehouses are {self.names}")
        assert _found == 1, NameError(f"More than one warehouse with name {name} was found: {_found}")
        return _found[0]


class ResultsListGetterMixin:
    results: List[Any]

    @property
    def names(self) -> List[str]:
        return [p.name for p in self.results]

    def get(self, name: str) -> Any:
        _found = list(filter(lambda p: p.name == name, self.results))
        assert _found, NameError(f"No objects with name {name} were found, available objects are {self.names}")
        assert _found == 1, NameError(f"More than one object with name {name} was found: {_found}")
        return _found[0]


class QueriesList(FlexibleModel, ResultsListGetterMixin):
    results: List[QueryInfo] = []


class DashboardsList(FlexibleModel, ResultsListGetterMixin):
    results: List[DashboardInfo] = []


class AlertsList(FlexibleModel, ResultsListGetterMixin):
    results: List[AlertInfo] = []


class SqlPropertiesAdjuster(ApiClientMixin, ElementSetterMixin):
    # TODO: design of this class is a terrible copy-paste. It must be rewritten.
    def __init__(self, api_client: ApiClient):
        super().__init__(api_client)
        self._warehouses = WarehousesList(**self.api_client.perform_query("GET", path="/sql/warehouses/"))

    def _adjust_warehouse_ref(self, element: str, parent: Any, index: Any):
        _id = self._warehouses.get(element.replace("warehouse://", "")).id
        self.set_element_at_parent(_id, parent, index)

    def _adjust_query_ref(self, element: str, parent: Any, index: Any):
        query_name = element.replace("query://", "")
        _relevant = QueriesList(
            **self.api_client.perform_query("GET", path="/preview/sql/queries", data={"q": f'name=="{query_name}"'})
        )
        _id = _relevant.get(query_name).id
        self.set_element_at_parent(_id, parent, index)

    def _adjust_dashboard_ref(self, element: str, parent: Any, index: Any):
        dashboard_name = element.replace("dashboard://", "")
        _relevant = DashboardsList(
            **self.api_client.perform_query(
                "GET", path="/preview/sql/dashboards", data={"q": f'name=="{dashboard_name}"'}
            )
        )
        _id = _relevant.get(dashboard_name).id
        self.set_element_at_parent(_id, parent, index)

    def _adjust_alert_ref(self, element: str, parent: Any, index: Any):
        alert_name = element.replace("alert://", "")
        _relevant = DashboardsList(
            **self.api_client.perform_query("GET", path="/preview/sql/alerts", data={"q": f'name=="{alert_name}"'})
        )
        _id = _relevant.get(alert_name).id
        self.set_element_at_parent(_id, parent, index)
