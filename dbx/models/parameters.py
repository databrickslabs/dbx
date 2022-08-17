from __future__ import annotations

import json
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, root_validator, validator

from dbx.models.task import validate_named_parameters

ParamPair = Dict[str, str]


class ExecuteWorkloadParamInfo(BaseModel):
    parameters: Optional[List[str]]  # for spark_python_task, python_wheel_task
    named_parameters: Optional[List[str]]  # for python_wheel_task

    @root_validator(pre=True)
    def initialize(cls, values: Dict[str, Any]):  # noqa
        if len(values) == 0:
            raise ValueError(f"No parameters were provided in the in the --parameters argument")
        if len(values) > 1:
            raise ValueError(
                f"More then one parameter payloads per one task or job provided in the --parameters argument"
            )

        parameter_name = list(values.keys())[0]

        if parameter_name not in cls.__fields__:
            raise ValueError(f"Unsupported parameter type {parameter_name} provided in the --parameters argument")

        return values

    @staticmethod
    def from_string(payload: str) -> ExecuteWorkloadParamInfo:
        return ExecuteWorkloadParamInfo(**json.loads(payload))

    @validator("named_parameters", pre=True)
    def _validate_named_parameters(cls, values: List[str]):  # noqa
        validate_named_parameters(values)
        return values
