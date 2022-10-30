from pathlib import Path
from typing import Any, Optional

import typer

from dbx.constants import DBX_CONFIGURE_DEFAULTS, PROJECT_INFO_FILE_PATH
from dbx.models.files.project import EnvironmentInfo, ProjectInfo
from dbx.utils import dbx_echo
from dbx.utils.json import JsonUtils


class JsonFileBasedManager:
    def __init__(self, file_path: Optional[Path] = PROJECT_INFO_FILE_PATH):
        self._file = file_path.absolute()

    def _read_typed(self) -> ProjectInfo:
        if not self._file.exists():
            raise FileNotFoundError(
                f"Project file {self._file} doesn't exist. Please verify that you're in the correct directory"
            )

        _content = JsonUtils.read(self._file)
        _typed = ProjectInfo(**_content)
        return _typed

    @staticmethod
    def _update_project_info_by_cli_params(project_info: ProjectInfo, typer_ctx: typer.Context):
        for param_name in DBX_CONFIGURE_DEFAULTS:
            value_from_cli = typer_ctx.params[param_name]
            dbx_echo(f'Setting "{param_name}" to: {value_from_cli}')
            setattr(project_info, param_name, value_from_cli)
            dbx_echo(f'✅ Setting "{param_name}" to: {value_from_cli}')

    def update(self, name: str, environment_info: EnvironmentInfo, typer_ctx: typer.Context):
        # for file-based manager it's the same logic
        self.create(name, environment_info, typer_ctx)

    def get(self, name: str) -> EnvironmentInfo:
        _typed = self._read_typed()
        return _typed.get_environment(name)

    def create(self, name: str, environment_info: EnvironmentInfo, typer_ctx: typer.Context):
        if self._file.exists():
            _info = self._read_typed()
            JsonFileBasedManager._update_project_info_by_cli_params(_info, typer_ctx)
            _info.environments.update({name: environment_info})
        else:
            _info = ProjectInfo(environments={name: environment_info})
            JsonFileBasedManager._update_project_info_by_cli_params(_info, typer_ctx)
            if not self._file.parent.exists():
                self._file.parent.mkdir(parents=True)
        JsonUtils.write(self._file, _info.dict())

    def create_or_update(self, name: str, environment_info: EnvironmentInfo, typer_ctx: typer.Context):
        if self._file.exists():
            self.update(name, environment_info, typer_ctx)
        else:
            self.create(name, environment_info, typer_ctx)

    def enable_jinja_support(self):
        _typed = self._read_typed()
        _typed.inplace_jinja_support = True
        JsonUtils.write(self._file, _typed.dict())

    def disable_jinja_support(self):
        _typed = self._read_typed()
        _typed.inplace_jinja_support = False
        JsonUtils.write(self._file, _typed.dict())

    def get_jinja_support(self) -> bool:
        _result = self._read_typed().inplace_jinja_support if self._file.exists() else False
        return _result

    def enable_failsafe_cluster_reuse(self):
        _typed = self._read_typed()
        _typed.failsafe_cluster_reuse_with_assets = True
        JsonUtils.write(self._file, _typed.dict())

    def get_failsafe_cluster_reuse(self):
        _result = self._read_typed().failsafe_cluster_reuse_with_assets if self._file.exists() else False
        return _result

    def enable_context_based_upload_for_execute(self):
        _typed = self._read_typed()
        _typed.context_based_upload_for_execute = True
        JsonUtils.write(self._file, _typed.dict())

    def get_context_based_upload_for_execute(self) -> bool:
        _result = self._read_typed().context_based_upload_for_execute if self._file.exists() else False
        return _result

    def get_param_value(self, param_name: str) -> Any:
        return getattr(self._read_typed(), param_name) if self._file.exists() else DBX_CONFIGURE_DEFAULTS[param_name]


class ProjectConfigurationManager:
    def __init__(self):
        self._manager = JsonFileBasedManager()

    def create_or_update(self, environment_name: str, environment_info: EnvironmentInfo, typer_ctx: typer.Context):
        self._manager.create_or_update(environment_name, environment_info, typer_ctx)

    def get(self, environment_name: str) -> EnvironmentInfo:
        return self._manager.get(environment_name)

    def enable_jinja_support(self):
        self._manager.enable_jinja_support()

    def disable_jinja_support(self):
        self._manager.disable_jinja_support()

    def get_jinja_support(self) -> bool:
        return self._manager.get_jinja_support()

    def enable_failsafe_cluster_reuse(self):
        self._manager.enable_failsafe_cluster_reuse()

    def get_failsafe_cluster_reuse(self) -> bool:
        return self._manager.get_failsafe_cluster_reuse()

    def enable_context_based_upload_for_execute(self):
        self._manager.enable_context_based_upload_for_execute()

    def get_context_based_upload_for_execute(self) -> bool:
        return self._manager.get_context_based_upload_for_execute()

    def get_param_value(self, param_name: str) -> Any:
        return self._manager.get_param_value(param_name)
