from pydantic import BaseModel


class ContextInfo(BaseModel):
    context_id: str
