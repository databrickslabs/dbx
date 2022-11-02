from abc import abstractmethod
from typing import Optional, Any

from dbx.api.adjuster.mixins.base import ApiClientMixin
from dbx.models.deployment import AnyWorkflow


class WorkflowBaseService(ApiClientMixin):
    @abstractmethod
    def find_by_name(self, name: str) -> Optional[int]:
        """Searches for the workflow by name and returns its id or None if not found"""

    @abstractmethod
    def create(self, wf: AnyWorkflow):
        """Creates the workflow from a given payload"""

    @abstractmethod
    def update(self, object_id: int, wf: AnyWorkflow):
        """Updates the workflow by provided id"""

    @abstractmethod
    def delete(self, object_id: Any):
        """Deletes the workflow by provided id"""
