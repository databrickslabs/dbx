from __future__ import annotations

import json
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, root_validator, validator

from dbx.models.parameters.common import validate_contains, validate_unique
from dbx.models.task import validate_named_parameters


class ExecuteWorkloadParamInfo(BaseModel):
    parameters: Optional[List[str]]  # for spark_python_task, python_wheel_task
    named_parameters: Optional[List[str]]  # only for python_wheel_task

    @root_validator(pre=True)
    def initialize(cls, values: Dict[str, Any]):  # noqa
        validate_contains(cls.__fields__, values)
        validate_unique(cls.__fields__, values)
        return values

    @staticmethod
    def from_string(payload: str) -> ExecuteWorkloadParamInfo:
        return ExecuteWorkloadParamInfo(**json.loads(payload))

    @validator("named_parameters", pre=True)
    def _validate_named_parameters(cls, values: List[str]):  # noqa
        validate_named_parameters(values)
        return values
