from pydantic.v1 import BaseModel


class ContextInfo(BaseModel):
    context_id: str
