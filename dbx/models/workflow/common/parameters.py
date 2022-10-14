from __future__ import annotations

from typing import Dict, List
from typing import Optional

from pydantic import BaseModel, validator

from dbx.models.validators import named_parameters_check

ParamPair = Optional[Dict[str, str]]
StringArray = Optional[List[str]]


class ParametersMixin(BaseModel):
    parameters: Optional[StringArray]


class NamedParametersMixin(BaseModel):
    named_parameters: Optional[StringArray]

    _named_check = validator("named_parameters", allow_reuse=True)(named_parameters_check)


class PipelineTaskParametersPayload(BaseModel):
    full_refresh: Optional[bool]


class BaseParametersMixin(BaseModel):
    base_parameters: Optional[ParamPair]


class StandardBasePayload(BaseModel):
    jar_params: Optional[StringArray]
    notebook_params: Optional[ParamPair]
    python_params: Optional[StringArray]
    spark_submit_params: Optional[StringArray]
