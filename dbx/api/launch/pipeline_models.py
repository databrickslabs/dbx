from enum import Enum
from typing import Optional, List

from pydantic import BaseModel


class UpdateStatus(str, Enum):
    ACTIVE = "ACTIVE"
    TERMINATED = "TERMINATED"


class PipelineGlobalState(str, Enum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"


class PipelineUpdateState(str, Enum):
    QUEUED = "QUEUED"
    CREATED = "CREATED"
    WAITING_FOR_RESOURCES = "WAITING_FOR_RESOURCES"
    INITIALIZING = "INITIALIZING"
    RESETTING = "RESETTING"
    SETTING_UP_TABLES = "SETTING_UP_TABLES"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"


class StartCause(str, Enum):
    API_CALL = "API_CALL"
    RETRY_ON_FAILURE = "RETRY_ON_FAILURE"
    SERVICE_UPGRADE = "SERVICE_UPGRADE"
    SCHEMA_CHANGE = "SCHEMA_CHANGE"
    JOB_TASK = "JOB_TASK"
    USER_ACTION = "USER_ACTION"


class LatestUpdate(BaseModel):
    update_id: str
    state: PipelineUpdateState
    cause: Optional[StartCause]


class PipelineUpdateStatus(BaseModel):
    status: Optional[UpdateStatus]
    latest_update: LatestUpdate


class PipelineDetails(BaseModel):
    state: Optional[PipelineGlobalState]
    latest_updates: Optional[List[LatestUpdate]] = []
