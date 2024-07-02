from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel

ParamPair = Optional[Dict[str, str]]
StringArray = Optional[List[str]]


class ParametersMixin(BaseModel):
    parameters: Optional[StringArray]


class NamedParametersMixin(BaseModel):
    named_parameters: Optional[Dict[str, Any]]


class PipelineTaskParametersPayload(BaseModel):
    full_refresh: Optional[bool]


class BaseParametersMixin(BaseModel):
    base_parameters: Optional[ParamPair]


class StandardBasePayload(BaseModel):
    jar_params: Optional[StringArray]
    notebook_params: Optional[ParamPair]
    python_params: Optional[StringArray]
    spark_submit_params: Optional[StringArray]
