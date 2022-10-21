from typing import Optional, List

from dbx.models.workflow.common.deployment_config import DbxDeploymentConfig
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.libraries import Library
from dbx.models.workflow.common.new_cluster import NewCluster
from dbx.models.workflow.common.job_email_notifications import JobEmailNotifications
from dbx.models.workflow.v2dot1.task import TaskMixin


class TaskDependencies(FlexibleModel):
    task_key: str


class JobTaskSettings(TaskMixin):
    task_key: str
    description: Optional[str]
    depends_on: Optional[List[TaskDependencies]]
    existing_cluster_id: Optional[str]
    new_cluster: Optional[NewCluster]
    job_cluster_key: Optional[str]
    libraries: Optional[List[Library]] = []
    email_notifications: Optional[JobEmailNotifications]
    timeout_seconds: Optional[int]
    max_retries: Optional[int]
    min_retry_interval_millis: Optional[int]
    retry_on_timeout: Optional[bool]
    deployment_config: Optional[DbxDeploymentConfig]
