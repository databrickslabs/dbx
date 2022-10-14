from abc import ABC
from enum import Enum
from typing import Optional

from dbx.models.validators import at_least_one_by_suffix, only_one_by_suffix
from dbx.models.workflow.common.parameters import ParamPair, StringArray
from dbx.models.workflow._flexible import FlexibleModel
from dbx.utils import dbx_echo
from pydantic import validator, root_validator


class BaseNotebookTask(FlexibleModel, ABC):
    notebook_path: str
    base_parameters: Optional[ParamPair]


class SparkJarTask(FlexibleModel):
    main_class_name: str
    parameters: Optional[StringArray]
    jar_uri: Optional[str]

    @validator("jar_uri")
    def _deprecated_msg(cls, value):  # noqa
        dbx_echo(
            "[yellow bold] Field jar_uri is DEPRECATED since 04/2016. "
            "Provide a [code]jar[/code] through the [code]libraries[/code] field instead."
        )
        return value


class SparkPythonTask(FlexibleModel):
    python_file: str
    parameters: Optional[StringArray]


class SparkSubmitTask(FlexibleModel):
    parameters: StringArray


class BasePipelineTask(FlexibleModel, ABC):
    pipeline_id: str


class TaskType(Enum):
    # task types defined both in v2.0 and v2.1
    notebook_task = "notebook_task"
    spark_jar_task = "spark_jar_task"
    spark_python_task = "spark_python_task"
    spark_submit_task = "spark_submit_task"
    pipeline_task = "pipeline_task"

    # specific to v2.1
    python_wheel_task = "python_wheel_task"
    sql_task = "sql_task"
    dbt_task = "dbt_task"

    # undefined handler for cases when a new task type is added
    undefined_task = "undefined_task"


class ApiVersion(str, Enum):
    v2dot0 = "v2dot0"
    v2dot1 = "v2dot1"

    undefined = "undefined"


class BaseTaskMixin(FlexibleModel, ABC):
    task_type: Optional[TaskType]
    api_version: Optional[ApiVersion]

    _at_least_one_check = root_validator(pre=True, allow_reuse=True)(
        lambda cls, values: at_least_one_by_suffix("_task", values)
    )
    _only_one_check = root_validator(pre=True, allow_reuse=True)(
        lambda cls, values: only_one_by_suffix("_task", values)
    )

    @validator("task_type", always=True)
    def task_type_validator(cls, v, values) -> TaskType:  # noqa

        for _type in TaskType:
            if values.get(_type.name):
                return TaskType(_type)

        return TaskType.undefined_task

    @validator("api_version", always=True)
    def get_api_version(cls, _) -> ApiVersion:  # noqa
        module_name = cls.__class__.__module__

        if ApiVersion.v2dot0 in module_name:
            return ApiVersion.v2dot0
        elif ApiVersion.v2dot1 in module_name:
            return ApiVersion.v2dot1
        else:
            return ApiVersion.undefined
