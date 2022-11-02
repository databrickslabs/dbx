from __future__ import annotations

from enum import Enum
from typing import Dict, Union, Optional

from pydantic import BaseModel

from dbx.constants import PROJECT_INFO_FILE_PATH
from dbx.utils import dbx_echo


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
    profile: str
    storage_type: Optional[StorageType] = StorageType.mlflow
    properties: MlflowStorageProperties

    @staticmethod
    def from_legacy(env: LegacyEnvironmentInfo) -> EnvironmentInfo:
        return EnvironmentInfo(
            profile=env.profile,
            storage_type=StorageType.mlflow,
            properties=MlflowStorageProperties(
                workspace_directory=env.workspace_dir, artifact_location=env.artifact_location
            ),
        )


class ProjectInfo(BaseModel):
    environments: Dict[str, Union[EnvironmentInfo, LegacyEnvironmentInfo]]
    inplace_jinja_support: Optional[bool] = False
    failsafe_cluster_reuse_with_assets: Optional[bool] = False
    context_based_upload_for_execute: Optional[bool] = False

    def get_environment(self, name: str) -> EnvironmentInfo:
        _env = self.environments.get(name)

        if not _env:
            raise NameError(f"Environment {name} not found in the project file {PROJECT_INFO_FILE_PATH}")

        if isinstance(_env, LegacyEnvironmentInfo):
            dbx_echo(
                "[yellow bold]Legacy environment format is used in project file. "
                "Please take a look at the docs and upgrade to the new format version.[/yellow bold]"
            )
            return EnvironmentInfo.from_legacy(_env)
        else:
            return _env
