from enum import Enum


class WorkflowType(str, Enum):
    pipeline = "pipeline"
    job = "job"
