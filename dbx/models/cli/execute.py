from __future__ import annotations

import json

from pydantic import root_validator

from dbx.models.validators import mutually_exclusive
from dbx.models.workflow.common.parameters import ParametersMixin, NamedParametersMixin


class ExecuteParametersPayload(ParametersMixin, NamedParametersMixin):
    """Parameters for execute"""

    _exclusive_check = root_validator(pre=True, allow_reuse=True)(
        lambda _, values: mutually_exclusive(["parameters", "named_parameters"], values)
    )

    @staticmethod
    def from_json(raw: str) -> ExecuteParametersPayload:
        return ExecuteParametersPayload(**json.loads(raw))
