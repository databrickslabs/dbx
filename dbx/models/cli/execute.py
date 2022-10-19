from __future__ import annotations

import json

from pydantic import root_validator

from dbx.models.validators import mutually_exclusive, at_least_one_of
from dbx.models.workflow.common.parameters import ParametersMixin, NamedParametersMixin


class ExecuteParametersPayload(ParametersMixin, NamedParametersMixin):
    """Parameters for execute"""

    @root_validator(pre=True)
    def _validate(cls, values):  # noqa
        at_least_one_of(["parameters", "named_parameters"], values)
        mutually_exclusive(["parameters", "named_parameters"], values)
        return values

    @staticmethod
    def from_json(raw: str) -> ExecuteParametersPayload:
        return ExecuteParametersPayload(**json.loads(raw))
