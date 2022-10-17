import collections
from typing import Optional, List, Dict, Any

from pydantic import root_validator, validator

from dbx.models.validators import at_least_one_of
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.access_control import AccessControlMixin
from dbx.models.workflow.common.workflow import WorkflowBase
from dbx.models.workflow.v2dot1.job_cluster import JobClustersMixin
from dbx.models.workflow.v2dot1.job_task_settings import JobTaskSettings
from dbx.models.workflow.v2dot1.parameters import AssetBasedRunPayload, StandardRunPayload


class GitSource(FlexibleModel):
    git_url: str
    git_provider: str
    git_branch: Optional[str]
    git_tag: Optional[str]
    git_commit: Optional[str]

    _one_of_provided = root_validator(allow_reuse=True)(
        lambda _, values: at_least_one_of(["git_branch", "git_tag", "git_commit"], values)
    )


class Workflow(WorkflowBase, AccessControlMixin, JobClustersMixin):
    # this follows the structure of 2.1 Jobs API
    # https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate
    tags: Optional[Dict[str, Any]]
    tasks: Optional[List[JobTaskSettings]]
    git_source: Optional[GitSource]
    format: Optional[str]

    @validator("tasks")
    def _validate_unique(cls, tasks: Optional[List[JobTaskSettings]]) -> Optional[List[JobTaskSettings]]:  # noqa
        if tasks:
            _duplicates = [
                name for name, count in collections.Counter([t.task_key for t in tasks]).items() if count > 1
            ]
            if _duplicates:
                raise ValueError(f"Duplicated task keys are not allowed. Provided payload: {tasks}")
        return tasks

    def get_task(self, name: str) -> JobTaskSettings:
        _found = list(filter(lambda t: t.name == name, self.tasks))
        return _found[0]

    @property
    def task_names(self) -> List[str]:
        return [t.task_key for t in self.tasks]

    def override_standard_launch_parameters(self, payload: StandardRunPayload):
        pass

    def override_asset_based_launch_parameters(self, payload: AssetBasedRunPayload):
        for task_parameters in payload.elements:
            _t = self.get_task(task_parameters.task_key)

            if not _t:
                raise ValueError(
                    f"Provided task key {task_parameters.task_key} doesn't exist in the workflow definition."
                    f"Available tasks are: {self.task_names}"
                )

            pointer = _t.__getattribute__(_t.task_type)
            pointer.__dict__.update(task_parameters.dict(exclude_none=True))
