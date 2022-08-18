from __future__ import annotations

from typing import Dict, Any

from pydantic import BaseModel, root_validator

from dbx.models.parameters.common import StringArray, ParamPair
from dbx.models.parameters.common import validate_contains


class RunNowV2d0ParamInfo(BaseModel):
    jar_params: StringArray
    python_params: StringArray
    spark_submit_params: StringArray
    notebook_params: ParamPair

    @root_validator(pre=True)
    def initialize(cls, values: Dict[str, Any]):  # noqa
        return validate_contains(cls.__fields__, values)


class RunNowV2d1ParamInfo(RunNowV2d0ParamInfo):
    python_named_params: ParamPair

    @root_validator(pre=True)
    def initialize(cls, values: Dict[str, Any]):  # noqa
        return validate_contains(cls.__fields__, values)
