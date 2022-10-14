from typing import Optional, List, Dict, Any

from dbx.models.validators import at_least_one_of
from dbx.models.workflow._flexible import FlexibleModel
from dbx.models.workflow.common.access_control import AccessControlMixin
from dbx.models.workflow.common.libraries import Library
from dbx.models.workflow.common.new_cluster import NewCluster
from dbx.models.workflow.common.workflow import JobEmailNotifications, WorkflowBase
from dbx.models.workflow.v2dot1.job_cluster import JobCluster
from dbx.models.workflow.v2dot1.task import TaskMixin
from pydantic import root_validator


class GitSource(FlexibleModel):
    git_url: str
    git_provider: str
    git_branch: Optional[str]
    git_tag: Optional[str]
    git_commit: Optional[str]

    _one_of_provided = root_validator(allow_reuse=True)(
        lambda _, values: at_least_one_of(["git_branch", "git_tag", "git_commit"], values)
    )


class TaskDependencies(FlexibleModel):
    task_key: str


class JobTaskSettings(TaskMixin):
    task_key: str
    description: Optional[str]
    depends_on: Optional[List[TaskDependencies]]
    existing_cluster_id: Optional[str]
    new_cluster: Optional[NewCluster]
    job_cluster_key: Optional[str]
    libraries: Optional[List[Library]]
    email_notifications: Optional[JobEmailNotifications]
    timeout_seconds: Optional[int]
    max_retries: Optional[int]
    min_retry_interval_millis: Optional[int]
    retry_on_timeout: Optional[bool]


class Workflow(WorkflowBase, AccessControlMixin):
    # this follows the structure of 2.1 Jobs API
    # https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate
    tags: Optional[Dict[str, Any]]
    tasks: Optional[List[JobTaskSettings]]
    job_clusters: Optional[List[JobCluster]]
    git_source: Optional[GitSource]
    format: Optional[str]
