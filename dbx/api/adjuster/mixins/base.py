from abc import ABC

from databricks_cli.sdk import ApiClient
from pydantic import BaseModel

from dbx.models.workflow.common.flexible import FlexibleModel


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
        if isinstance(parent, (dict, list)):
            parent[index] = element
        elif isinstance(parent, (BaseModel, FlexibleModel)):
            setattr(parent, index, element)
        else:
            raise ValueError(
                "Cannot apply reference to the parent structure."
                f"Please create a GitHub issue providing the following parent object type: {type(parent)}"
            )
