from typing import Union

from dbx.models.workflow.v2dot0.workflow import Workflow as V2dot0Workflow
from dbx.models.workflow.v2dot1.job_task_settings import JobTaskSettings

ExecuteTask = Union[V2dot0Workflow, JobTaskSettings]
