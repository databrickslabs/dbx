from __future__ import annotations

import json
from typing import List, Optional

from pydantic import BaseModel, validator

from dbx.models.validators import check_dbt_commands
from dbx.models.workflow.common.parameters import (
    ParamPair,
    PipelineTaskParametersPayload,
    StandardBasePayload,
    StringArray,
)
from dbx.models.workflow.v2dot1._parameters import PayloadElement


class AssetBasedRunPayload(BaseModel):
    elements: Optional[List[PayloadElement]]

    @staticmethod
    def from_string(raw: str) -> AssetBasedRunPayload:
        return AssetBasedRunPayload(elements=json.loads(raw))


class StandardRunPayload(StandardBasePayload):
    python_named_params: Optional[ParamPair]
    pipeline_params: Optional[PipelineTaskParametersPayload]
    sql_params: Optional[ParamPair]
    dbt_commands: Optional[StringArray]

    _verify_dbt_commands = validator("dbt_commands", allow_reuse=True)(check_dbt_commands)
