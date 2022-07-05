from pathlib import Path

from pydantic import BaseModel


class TempStorage(BaseModel):
    driver_path: Path


class ServerInfo(BaseModel):
    status: str = "working"
    temp_storage: TempStorage
