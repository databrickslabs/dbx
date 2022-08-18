from __future__ import annotations

import json
from copy import deepcopy
from typing import Optional, List, Dict, Any, Union

from pydantic import BaseModel, root_validator, validator

from dbx.models.task import validate_named_parameters

ParamPair = Dict[str, str]


def validate_any_field(fields, values: Dict[str, Any]):
    _values = deepcopy(values)
    if "task_key" in _values:
        _values.pop("task_key")

    if len(_values) == 0:
        raise ValueError(f"No parameters were provided in the in the --parameters argument, values content: {values}")
    if len(_values) > 1:
        raise ValueError(
            f"More then one parameter payloads per one task or job provided in the --parameters argument."
            f"Values content: {values}"
        )

    parameter_name = list(_values.keys())[0]

    if parameter_name not in fields:
        raise ValueError(f"Unsupported parameter type {parameter_name} provided in the --parameters argument")

    return values


class ExecuteWorkloadParamInfo(BaseModel):
    parameters: Optional[List[str]]  # for spark_python_task, python_wheel_task
    named_parameters: Optional[List[str]]  # only for python_wheel_task

    @root_validator(pre=True)
    def initialize(cls, values: Dict[str, Any]):  # noqa
        return validate_any_field(cls.__fields__, values)

    @staticmethod
    def from_string(payload: str) -> ExecuteWorkloadParamInfo:
        return ExecuteWorkloadParamInfo(**json.loads(payload))

    @validator("named_parameters", pre=True)
    def _validate_named_parameters(cls, values: List[str]):  # noqa
        validate_named_parameters(values)
        return values


class TaskDefinition(BaseModel):
    base_parameters: Optional[ParamPair]  # for notebook_task
    parameters: Optional[List[str]]  # for spark_jar_task, spark_python_task, spark_submit_task, python_wheel_task
    named_parameters: Optional[List[str]]  # for python_wheel_task

    @root_validator(pre=True)
    def initialize(cls, values: Dict[str, Any]):  # noqa
        return validate_any_field(cls.__fields__, values)

    @validator("named_parameters", pre=True)
    def _validate_named_parameters(cls, values: List[str]):  # noqa
        validate_named_parameters(values)
        return values


class NamedTaskDefinition(TaskDefinition):
    task_key: str


class LaunchWorkloadParamInfo(BaseModel):
    content: Union[TaskDefinition, List[NamedTaskDefinition]]

    @staticmethod
    def from_string(payload: str) -> LaunchWorkloadParamInfo:
        return LaunchWorkloadParamInfo(**{"content": json.loads(payload)})
