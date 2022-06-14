from __future__ import annotations

import abc
from pathlib import Path
from typing import Dict, Union, List, Optional

from pydantic import BaseModel

from dbx.utils.cli import parse_list_of_arguments


class ArtifactStorageInfo(BaseModel, abc.ABC):
    storage_type: str
    properties: BaseModel


class MlflowArtifactStorageProperties(BaseModel):
    workspace_directory: str
    artifact_location: str

    @staticmethod
    def get_default_properties() -> Dict[str, str]:
        current_path_name = Path(".").absolute().name
        props = {
            "workspace_directory": f"/Shared/dbx/projects/{current_path_name}",
            "artifact_location": f"dbfs:/dbx/{current_path_name}",
        }
        return props

    @classmethod
    def parse_from_provided(cls, properties: Optional[List[str]] = None) -> MlflowArtifactStorageProperties:
        _properties = cls.get_default_properties()
        _parsed = parse_list_of_arguments(properties) if properties else {}
        _properties.update(_parsed)
        return cls.parse_obj(_properties)


class MlflowArtifactStorageInfo(ArtifactStorageInfo):
    storage_type = "mlflow"
    properties: MlflowArtifactStorageProperties


class Project(BaseModel):
    environments: Dict[str, Union[MlflowArtifactStorageInfo]]
