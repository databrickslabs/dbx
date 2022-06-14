import abc
from typing import Dict, Union

from pydantic import BaseModel


class ArtifactStorageInfo(BaseModel, abc.ABC):
    storage_type: str
    properties: BaseModel


class MlflowArtifactStorageProperties(BaseModel):
    workspace_directory: str
    artifact_location: str


class MlflowArtifactStorageInfo(ArtifactStorageInfo):
    storage_type = "mlflow"
    properties: MlflowArtifactStorageProperties


class Project(BaseModel):
    environments: Dict[str, Union[MlflowArtifactStorageInfo]]
