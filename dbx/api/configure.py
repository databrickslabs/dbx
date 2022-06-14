from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Union

from dbx.constants import INFO_FILE_PATH
from dbx.models.project import ArtifactStorageInfo, Project, MlflowArtifactStorageInfo
from dbx.utils.json import JsonUtils


class _ProjectManager(ABC):
    @abstractmethod
    def create(self, name: str, info: ArtifactStorageInfo):
        """"""

    @abstractmethod
    def update(self, name: str, info: ArtifactStorageInfo):
        """"""

    @abstractmethod
    def get(self, name: str) -> Optional[Union[MlflowArtifactStorageInfo]]:
        """"""

    def create_or_update(self, name: str, info: ArtifactStorageInfo):
        if self.get(name):
            self.update(name, info)
        else:
            self.create(name, info)


class _JsonFileBasedManager(_ProjectManager):
    def __init__(self, file_path: Optional[Path] = INFO_FILE_PATH):
        self._file = file_path.absolute()
        if not self._file.parent.exists():
            self._file.parent.mkdir(parents=True)

    @property
    def _file_content(self) -> Optional[Project]:
        if self._file.exists():
            return Project(**JsonUtils.read(self._file))
        else:
            return None

    @_file_content.setter
    def _file_content(self, content: Project):
        JsonUtils.write(self._file, content.dict())

    def update(self, name: str, info: ArtifactStorageInfo):
        # for file-based manager it's the same logic
        self.create(name, info)

    def get(self, name: str) -> Optional[ArtifactStorageInfo]:
        if self._file.exists():
            return self._file_content.environments.get(name)
        else:
            return None

    def create(self, name: str, info: ArtifactStorageInfo):
        if self._file_content:
            _new = self._file_content.copy()
            _new.environments.update({name: info})
        else:
            _new = Project(environments={name: info})
        self._file_content = _new


class ProjectConfigurationManager:
    def __init__(self, underlying_manager: Optional[_ProjectManager] = None):
        self._manager = underlying_manager if underlying_manager else _JsonFileBasedManager()

    def create_or_update(self, environment_name: str, storage_info: ArtifactStorageInfo):
        self._manager.create_or_update(environment_name, storage_info)

    def get(self, environment_name: str) -> Union[MlflowArtifactStorageInfo]:
        return self._manager.get(environment_name)
