from pydantic import root_validator

from dbx.models.validators import mutually_exclusive
from dbx.models.workflow.common.parameters import BaseParametersMixin, ParametersMixin, StandardBasePayload


class AssetBasedRunPayload(BaseParametersMixin, ParametersMixin):
    """"""

    _validate_unique = root_validator(pre=True)(
        lambda _, values: mutually_exclusive(["base_parameters", "parameters"], values)
    )


class StandardRunPayload(StandardBasePayload):
    """"""
