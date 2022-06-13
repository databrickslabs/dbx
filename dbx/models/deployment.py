from typing import List, Dict

from pydantic import BaseModel

from dbx.models.tasks import TaskDefinition


class WorkloadDefinition(BaseModel):
    name: str
    tasks: List[TaskDefinition]

    class Config:
        smart_union = True


class Environment(BaseModel):
    workloads: List[WorkloadDefinition]


class Deployments(BaseModel):
    environments: Dict[str, Environment]
