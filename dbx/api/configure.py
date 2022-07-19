from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Dict, Any

from dbx.constants import INFO_FILE_PATH
from dbx.utils.json import JsonUtils


class EnvironmentInfo:
    @property
    def _current_path_name(self) -> str:
        return Path(".").absolute().name

    def __init__(self, profile: str, workspace_dir: Optional[str] = None, artifact_location: Optional[str] = None):
        self.profile = profile
        self.workspace_dir = f"/Shared/dbx/projects/{self._current_path_name}" if not workspace_dir else workspace_dir
        self.artifact_location = f"dbfs:/dbx/{self._current_path_name}" if not artifact_location else artifact_location

    def as_dict(self) -> Dict[str, str]:
        return {
            "profile": self.profile,
            "workspace_dir": self.workspace_dir,
            "artifact_location": self.artifact_location,
        }


class EnvironmentDataManager(ABC):
    @abstractmethod
    def create(self, name: str, environment_info: EnvironmentInfo):
        """"""

    @abstractmethod
    def update(self, name: str, environment_info: EnvironmentInfo):
        """"""

    @abstractmethod
    def get(self, name: str) -> Optional[EnvironmentInfo]:
        """"""

    def create_or_update(self, name: str, environment_info: EnvironmentInfo):
        if self.get(name):
            self.update(name, environment_info)
        else:
            self.create(name, environment_info)


class JsonFileBasedManager(EnvironmentDataManager):
    def __init__(self, file_path: Optional[Path] = INFO_FILE_PATH):
        self._file = file_path.absolute()
        if not self._file.parent.exists():
            self._file.parent.mkdir(parents=True)

    @property
    def _file_content(self) -> Dict[str, EnvironmentInfo]:
        if not self._file.exists():
            return {}
        else:
            _raw: Dict = JsonUtils.read(self._file).get("environments", {})
            _typed = {name: EnvironmentInfo(**value) for name, value in _raw.items()}
            return _typed

    @_file_content.setter
    def _file_content(self, content: Dict[str, EnvironmentInfo]):
        _untyped: Dict[str, Any] = {name: value.as_dict() for name, value in content.items()}
        with_environments = {"environments": _untyped}
        JsonUtils.write(self._file, with_environments)

    def update(self, name: str, environment_info: EnvironmentInfo):
        # for file-based manager it's the same logic
        self.create(name, environment_info)

    def get(self, name: str) -> Optional[EnvironmentInfo]:
        return self._file_content.get(name)

    def create(self, name: str, environment_info: EnvironmentInfo):
        _new = self._file_content.copy()
        _new.update({name: environment_info})
        self._file_content = _new


class ConfigurationManager:
    def __init__(self, underlying_manager: Optional[EnvironmentDataManager] = None):
        self._manager = underlying_manager if underlying_manager else JsonFileBasedManager()

    def create_or_update(self, environment_name: str, environment_info: EnvironmentInfo):
        self._manager.create_or_update(environment_name, environment_info)

    def get(self, environment_name: str) -> Optional[EnvironmentInfo]:
        return self._manager.get(environment_name)
