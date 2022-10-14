from typing import Optional, List

from dbx.models.workflow.common.workflow import WorkflowBase
from dbx.models.workflow.common.libraries import Library
from dbx.models.workflow.common.new_cluster import NewCluster
from dbx.models.workflow.v2dot0.task import TaskMixin
from pydantic import root_validator, validator


class Workflow(WorkflowBase, TaskMixin):
    # this follows structure of 2.0 API
    # https://docs.databricks.com/dev-tools/api/2.0/jobs.html
    existing_cluster_id: Optional[str]
    existing_cluster_name: Optional[str]  # deprecated field
    new_cluster: Optional[NewCluster]
    libraries: Optional[List[Library]]
    max_retries: Optional[int]
    min_retry_interval_millis: Optional[int]
    retry_on_timeout: Optional[bool]

    @validator("existing_cluster_name")
    def _deprecated(cls, value):  # noqa
        cls.field_deprecated("existing_cluster_id", "existing_cluster_name", "cluster", value)
        return value

    @root_validator(pre=True)
    def mutually_exclusive(cls, values):  # noqa
        if not cls.task_type.pipeline_task:
            if values.get("new_cluster") and (values.get("existing_cluster_id") or values.get("existing_cluster_name")):
                raise ValueError(
                    'Fields ("existing_cluster_id" or "existing_cluster_name") and "new_cluster" are mutually exclusive'
                )
