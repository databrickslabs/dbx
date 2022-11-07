from enum import Enum
from typing import Optional, List

from pydantic import root_validator, validator, BaseModel
from pydantic.fields import Field

from dbx.models.validators import check_dbt_commands, at_least_one_of, mutually_exclusive, named_parameters_check
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.task import (
    BaseTaskMixin,
    BaseNotebookTask,
    SparkJarTask,
    SparkPythonTask,
    SparkSubmitTask,
    BasePipelineTask,
)


class NotebookSource(str, Enum):
    WORKSPACE = "WORKSPACE"
    GIT = "GIT"


class NotebookTask(BaseNotebookTask):
    source: Optional[NotebookSource]


class PipelineTask(BasePipelineTask):
    full_refresh: Optional[bool]


class SqlTaskQuery(FlexibleModel):
    query_id: str


class SqlTaskDashboard(FlexibleModel):
    dashboard_id: str


class SqlTaskAlert(FlexibleModel):
    alert_id: str


class SqlTask(FlexibleModel):
    warehouse_id: str
    query: Optional[SqlTaskQuery]
    dashboard: Optional[SqlTaskDashboard]
    alert: Optional[SqlTaskAlert]

    @root_validator(pre=True)
    def _validate(cls, values):  # noqa
        at_least_one_of(["query", "dashboard", "alert"], values)
        mutually_exclusive(["query", "dashboard", "alert"], values)
        return values


class DbtTask(FlexibleModel):
    project_directory: Optional[str]
    profiles_directory: Optional[str]
    commands: List[str]
    _schema: str = Field(alias="schema")  # noqa
    warehouse_id: str

    _verify_dbt_commands = validator("commands", allow_reuse=True)(check_dbt_commands)


class PythonWheelTask(BaseModel):
    package_name: str
    entry_point: str
    parameters: Optional[List[str]] = []
    named_parameters: Optional[List[str]] = []

    _validate_exclusive = root_validator(pre=True, allow_reuse=True)(
        lambda _, values: mutually_exclusive(["parameters", "named_parameters"], values)
    )

    _named_check = validator("named_parameters", allow_reuse=True)(named_parameters_check)


class TaskMixin(BaseTaskMixin):
    notebook_task: Optional[NotebookTask]
    spark_jar_task: Optional[SparkJarTask]
    spark_python_task: Optional[SparkPythonTask]
    spark_submit_task: Optional[SparkSubmitTask]
    python_wheel_task: Optional[PythonWheelTask]
    pipeline_task: Optional[PipelineTask]
    sql_task: Optional[SqlTask]
    dbt_task: Optional[DbtTask]
