from typing import Optional, List, Union, Literal

from pydantic import root_validator, validator

from dbx.models.workflow.common.access_control import AccessControlMixin
from dbx.models.workflow.common.deployment_config import DbxDeploymentConfig
from dbx.models.workflow.common.libraries import Library
from dbx.models.workflow.common.new_cluster import NewCluster
from dbx.models.workflow.common.task import SparkPythonTask, SparkJarTask, SparkSubmitTask
from dbx.models.workflow.common.task_type import TaskType
from dbx.models.workflow.common.workflow import WorkflowBase
from dbx.models.workflow.common.workflow_types import WorkflowType
from dbx.models.workflow.v2dot0.parameters import AssetBasedRunPayload
from dbx.models.workflow.v2dot0.task import TaskMixin, NotebookTask

ALLOWED_TASK_TYPES = Union[SparkPythonTask, NotebookTask, SparkJarTask, SparkSubmitTask]


class Workflow(WorkflowBase, TaskMixin, AccessControlMixin):
    # this follows structure of 2.0 API
    # https://docs.databricks.com/dev-tools/api/2.0/jobs.html
    existing_cluster_id: Optional[str]
    existing_cluster_name: Optional[str]  # deprecated field
    new_cluster: Optional[NewCluster]
    libraries: Optional[List[Library]] = []
    max_retries: Optional[int]
    min_retry_interval_millis: Optional[int]
    retry_on_timeout: Optional[bool]
    deployment_config: Optional[DbxDeploymentConfig]
    workflow_type: Literal[WorkflowType.job_v2d0] = WorkflowType.job_v2d0

    @validator("existing_cluster_name")
    def _deprecated(cls, value):  # noqa
        cls.field_deprecated("existing_cluster_id", "existing_cluster_name", "cluster", value)
        return value

    @root_validator()
    def mutually_exclusive(cls, values):  # noqa
        if not values.get("pipeline_task"):
            if values.get("new_cluster") and (values.get("existing_cluster_id") or values.get("existing_cluster_name")):
                raise ValueError(
                    'Fields ("existing_cluster_id" or "existing_cluster_name") and "new_cluster" are mutually exclusive'
                )
        return values

    def get_task(self, task_key: str):
        raise RuntimeError("Provided workflow format is V2.0, and it doesn't support tasks")

    def override_asset_based_launch_parameters(self, payload: AssetBasedRunPayload):
        if self.task_type == TaskType.notebook_task:
            self.notebook_task.base_parameters = payload.base_parameters
        else:
            pointer = getattr(self, self.task_type)
            pointer.__dict__.update(payload.dict(exclude_none=True))
