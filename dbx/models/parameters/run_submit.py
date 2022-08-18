from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, validator, root_validator

from dbx.models.parameters.common import ParamPair, StringArray, validate_contains, validate_unique
from dbx.models.task import validate_named_parameters


class NotebookTask(BaseModel):
    base_parameters: ParamPair


class SparkJarTask(BaseModel):
    parameters: StringArray


class SparkPythonTask(BaseModel):
    parameters: StringArray


class SparkSubmitTask(BaseModel):
    parameters: StringArray


class PythonWheelTask(BaseModel):
    parameters: StringArray
    named_parameters: ParamPair

    @root_validator(pre=True)
    def initialize(cls, values):  # noqa
        validate_contains(cls.__fields__, values)
        validate_unique(cls.__fields__, values)
        return values

    @validator("named_parameters", pre=True)
    def _validate_named_parameters(cls, values: List[str]):  # noqa
        validate_named_parameters(values)
        return values


class RunSubmitV2d0ParamInfo(BaseModel):
    notebook_task: Optional[NotebookTask]
    spark_jar_task: Optional[SparkJarTask]
    spark_python_task: Optional[SparkPythonTask]
    spark_submit_task: Optional[SparkSubmitTask]


class NamedV2d1Task(BaseModel):
    task_key: str
    notebook_task: Optional[NotebookTask]
    spark_jar_task: Optional[SparkJarTask]
    spark_python_task: Optional[SparkPythonTask]
    spark_submit_task: Optional[SparkSubmitTask]
    python_wheel_task: Optional[PythonWheelTask]

    @root_validator(pre=True)
    def initialize(cls, values):  # noqa
        task_fields = {k: v for k, v in cls.__fields__.items() if k != "task_key"}
        validate_contains(task_fields, values)
        validate_unique(task_fields, values)
        return values


class RunSubmitV2d1ParamInfo(BaseModel):
    tasks: List[NamedV2d1Task]
