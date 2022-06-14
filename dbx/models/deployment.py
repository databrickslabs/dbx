from typing import List, Dict, Optional, Any

from pydantic import BaseModel

from dbx.models.clusters import JobCluster
from dbx.models.tasks import TaskDefinition


class WorkloadDefinition(BaseModel):
    name: str
    job_clusters: Optional[List[JobCluster]]
    tasks: List[TaskDefinition]

    def to_api_format(self) -> Dict[str, Any]:
        return self.dict()


class Environment(BaseModel):
    workloads: List[WorkloadDefinition]


class Deployment(BaseModel):
    """
    Deployment is the top-level model which contains all environment definitions.
    Environments contain workloads, and workloads contain task definitions.
    """

    environments: Dict[str, Environment]
