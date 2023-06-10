from __future__ import annotations

from typing import Optional, Union

from dbx.models.workflow.common.parameters import (
    BaseParametersMixin,
    NamedParametersMixin,
    ParametersMixin,
    ParamPair,
    PipelineTaskParametersPayload,
    StringArray,
)


class FlexibleParametersMixin(ParametersMixin):
    parameters: Optional[Union[ParamPair, StringArray]]


class PayloadElement(FlexibleParametersMixin, BaseParametersMixin, PipelineTaskParametersPayload, NamedParametersMixin):
    task_key: str
