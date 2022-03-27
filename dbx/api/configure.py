import json
import pathlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional, Dict

from dbx.constants import INFO_FILE_PATH


def _current_path_name() -> str:
    return Path(".").absolute().name


@dataclass
class EnvironmentInfo:
    profile: str
    workspace_dir: Optional[str] = f"/Shared/dbx/projects/{_current_path_name()}"
    artifact_location: Optional[str] = f"dbfs:/dbx/{_current_path_name()}"


class EnvironmentDataManager(ABC):
    @abstractmethod
    def create(self, name: str, environment_info: EnvironmentInfo):
        pass

    @abstractmethod
    def update(self, name: str, environment_info: EnvironmentInfo):
        pass

    @abstractmethod
    def get(self, name: str) -> Optional[EnvironmentInfo]:
        pass

    def create_or_update(self, name: str, environment_info: EnvironmentInfo):
        if self.get(name):
            self.update(name, environment_info)
        else:
            self.create(name, environment_info)


class FileBasedManager(EnvironmentDataManager):
    def __init__(self, file_path: Optional[str] = INFO_FILE_PATH):
        self._file = pathlib.Path(file_path)
        if not self._file.parent.exists():
            self._file.parent.mkdir(parents=True)

    @property
    def _file_content(self) -> Dict[str, EnvironmentInfo]:
        if not self._file.exists():
            return {}
        else:
            _raw: Dict = json.loads(self._file.read_text(encoding="utf-8")).get("environments", {})
            _typed = {name: EnvironmentInfo(**value) for name, value in _raw.items()}
            return _typed

    @_file_content.setter
    def _file_content(self, content: Dict[EnvironmentInfo]):
        _untyped: Dict = {name: asdict(value) for name, value in content.items()}
        _jsonified = json.dumps(_untyped, indent=4)
        self._file.write_text(_jsonified, encoding="utf-8")

    def update(self, name: str, environment_info: EnvironmentInfo):
        # for file-based manager it's the same logic
        self.create(name, environment_info)

    def get(self, name: str) -> Optional[EnvironmentInfo]:
        return self._file_content.get(name)

    def create(self, name: str, environment_info: EnvironmentInfo):
        _content = self._file_content.update({name: environment_info})
        self._file_content = _content


class ConfigurationManager:
    def __init__(self, underlying_manager: Optional[EnvironmentDataManager] = FileBasedManager()):
        self._manager = underlying_manager

    def create_or_update(self, environment_name: str, environment_info: EnvironmentInfo):
        self._manager.create_or_update(environment_name, environment_info)

    def get(self, environment_name: str) -> Optional[EnvironmentInfo]:
        return self._manager.get(environment_name)
