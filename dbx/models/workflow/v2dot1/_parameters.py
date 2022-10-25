from __future__ import annotations

from typing import Optional, Union

from dbx.models.workflow.common.parameters import (
    ParametersMixin,
    ParamPair,
    StringArray,
    BaseParametersMixin,
    PipelineTaskParametersPayload,
)


class FlexibleParametersMixin(ParametersMixin):
    parameters: Optional[Union[ParamPair, StringArray]]


class PayloadElement(FlexibleParametersMixin, BaseParametersMixin, PipelineTaskParametersPayload):
    task_key: str
