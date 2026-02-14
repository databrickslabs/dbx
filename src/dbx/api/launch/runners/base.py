from typing import Optional

from pydantic.v1 import BaseModel


class RunData(BaseModel):
    run_id: Optional[int]


class PipelineUpdateResponse(BaseModel):
    update_id: str
    request_id: str
