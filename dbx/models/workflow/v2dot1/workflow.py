import collections
from typing import Optional, List, Dict, Any, Literal

from pydantic import root_validator, validator

from dbx.models.validators import at_least_one_of, mutually_exclusive
from dbx.models.workflow.common.access_control import AccessControlMixin
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.workflow import WorkflowBase
from dbx.models.workflow.common.workflow_types import WorkflowType
from dbx.models.workflow.v2dot1.job_cluster import JobClustersMixin
from dbx.models.workflow.v2dot1.job_task_settings import JobTaskSettings
from dbx.models.workflow.v2dot1.parameters import AssetBasedRunPayload
from dbx.utils import dbx_echo


class GitSource(FlexibleModel):
    git_url: str
    git_provider: str
    git_branch: Optional[str]
    git_tag: Optional[str]
    git_commit: Optional[str]

    @root_validator(pre=True)
    def _validate(cls, values):  # noqa
        at_least_one_of(["git_branch", "git_tag", "git_commit"], values)
        mutually_exclusive(["git_branch", "git_tag", "git_commit"], values)
        return values


class Workflow(WorkflowBase, AccessControlMixin, JobClustersMixin):
    # this follows the structure of 2.1 Jobs API
    # https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate
    tags: Optional[Dict[str, Any]]
    tasks: Optional[List[JobTaskSettings]]
    git_source: Optional[GitSource]
    format: Optional[str]
    workflow_type: Literal[WorkflowType.job_v2d1] = WorkflowType.job_v2d1

    @validator("tasks")
    def _validate_tasks(cls, tasks: Optional[List[JobTaskSettings]]) -> Optional[List[JobTaskSettings]]:  # noqa
        if tasks:
            _duplicates = [
                name for name, count in collections.Counter([t.task_key for t in tasks]).items() if count > 1
            ]
            if _duplicates:
                raise ValueError("Duplicated task keys are not allowed.")
        else:
            dbx_echo(
                "[yellow bold]No task definitions were provided for workflow. "
                "This might cause errors during deployment[/yellow bold]"
            )
        return tasks

    def get_task(self, task_key: str) -> JobTaskSettings:
        _found = list(filter(lambda t: t.task_key == task_key, self.tasks))
        assert len(_found) == 1, ValueError(
            f"Requested task key {task_key} doesn't exist in the workflow definition."
            f"Available tasks are: {self.task_names}"
        )
        return _found[0]

    @property
    def task_names(self) -> List[str]:
        return [t.task_key for t in self.tasks]

    def override_asset_based_launch_parameters(self, payload: AssetBasedRunPayload):
        for task_parameters in payload.elements:
            _t = self.get_task(task_parameters.task_key)
            pointer = getattr(_t, _t.task_type)
            pointer.__dict__.update(task_parameters.dict(exclude_none=True))
