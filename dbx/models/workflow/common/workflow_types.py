from enum import Enum


class WorkflowType(str, Enum):
    pipeline = "pipeline"
    job_v2d0 = "job-v2.0"
    job_v2d1 = "job-v2.1"
