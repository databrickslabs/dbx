from __future__ import annotations

from enum import Enum
from typing import Dict, Union

from pydantic import BaseModel


class StorageType(str, Enum):
    mlflow = "mlflow"


class LegacyEnvironmentInfo(BaseModel):
    profile: str
    workspace_dir: str
    artifact_location: str


class MlflowStorageProperties(BaseModel):
    workspace_directory: str
    artifact_location: str


class EnvironmentInfo(BaseModel):
    storage_type: StorageType
    properties: Union[MlflowStorageProperties]


class LegacyProjectInfo(BaseModel):
    environments: Dict[str, LegacyEnvironmentInfo]


class ProjectInfo(BaseModel):
    environments: Dict[str, EnvironmentInfo]

    @classmethod
    def from_legacy(cls, legacy: LegacyProjectInfo) -> ProjectInfo:
        _container = {}
        for name, env in legacy.environments.items():
            _container[name] = EnvironmentInfo(
                storage_type=StorageType.mlflow,
                properties=MlflowStorageProperties(
                    workspace_directory=env.workspace_dir, artifact_location=env.artifact_location
                ),
            )
        return ProjectInfo(environments=_container)
