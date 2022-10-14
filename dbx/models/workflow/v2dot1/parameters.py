from __future__ import annotations

import json
from typing import Optional, List

from pydantic import BaseModel, validator

from dbx.models.validators import validate_dbt_commands
from dbx.models.workflow.common.parameters import (
    ParamPair,
    StringArray,
    StandardBasePayload,
    PipelineTaskParametersPayload,
)
from dbx.models.workflow.v2dot1._parameters import PayloadElement


class AssetBasedParametersPayload(BaseModel):
    elements: Optional[List[PayloadElement]]

    @staticmethod
    def from_string(raw: str) -> AssetBasedParametersPayload:
        return AssetBasedParametersPayload(elements=json.loads(raw))


class StandardRunPayload(StandardBasePayload):
    python_named_params: Optional[ParamPair]
    pipeline_params: Optional[PipelineTaskParametersPayload]
    sql_params: Optional[ParamPair]
    dbt_commands: Optional[StringArray]

    _validate_commands = validator("commands", allow_reuse=True)(validate_dbt_commands)
