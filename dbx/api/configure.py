from pathlib import Path
from typing import Optional

from dbx.constants import PROJECT_INFO_FILE_PATH
from dbx.models.project import EnvironmentInfo, ProjectInfo
from dbx.utils.json import JsonUtils


class JsonFileBasedManager:
    def __init__(self, file_path: Optional[Path] = PROJECT_INFO_FILE_PATH):
        self._file = file_path.absolute()
        if not self._file.parent.exists():
            self._file.parent.mkdir(parents=True)

    def _read_typed(self) -> ProjectInfo:
        _content = JsonUtils.read(self._file)
        _typed = ProjectInfo(**_content)
        return _typed

    def update(self, name: str, environment_info: EnvironmentInfo):
        # for file-based manager it's the same logic
        self.create(name, environment_info)

    def get(self, name: str) -> Optional[EnvironmentInfo]:
        if not self._file.exists():
            raise FileNotFoundError(
                f"Project file {self._file} doesn't exist. Please verify that you're in the correct directory"
            )

        _typed = self._read_typed()
        return _typed.get_environment(name)

    def create(self, name: str, environment_info: EnvironmentInfo):
        if self._file.exists():
            _info = self._read_typed()
            _info.environments.update({name: environment_info})
        else:
            _info = ProjectInfo(environments={name: environment_info})

        JsonUtils.write(self._file, _info.dict())

    def create_or_update(self, name: str, environment_info: EnvironmentInfo):
        if self._file.exists() and self.get(name):
            self.update(name, environment_info)
        else:
            self.create(name, environment_info)


class ConfigurationManager:
    def __init__(self):
        self._manager = JsonFileBasedManager()

    def create_or_update(self, environment_name: str, environment_info: EnvironmentInfo):
        self._manager.create_or_update(environment_name, environment_info)

    def get(self, environment_name: str) -> Optional[EnvironmentInfo]:
        return self._manager.get(environment_name)
