import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Dict, Any

import jinja2
import yaml

import dbx.api.jinja as dbx_jinja
from dbx.api._module_loader import load_module_from_source
from dbx.api.configure import ProjectConfigurationManager
from dbx.constants import CUSTOM_JINJA_FUNCTIONS_PATH
from dbx.models.deployment import DeploymentConfig, EnvironmentDeploymentInfo
from dbx.utils import dbx_echo
from dbx.utils.json import JsonUtils


class _AbstractConfigReader(ABC):
    def __init__(self, path: Path):
        self._path = path
        self.config = self._get_config()

    def _get_config(self) -> DeploymentConfig:
        return self._read_file()

    @abstractmethod
    def _read_file(self) -> DeploymentConfig:
        """"""


class YamlConfigReader(_AbstractConfigReader):
    def _read_file(self) -> DeploymentConfig:
        content = yaml.load(self._path.read_text(encoding="utf-8"), yaml.SafeLoader)
        return DeploymentConfig.from_payload(content)


class JsonConfigReader(_AbstractConfigReader):
    def _read_file(self) -> DeploymentConfig:
        _content = JsonUtils.read(self._path)
        return self.read_content(_content)

    @staticmethod
    def read_content(content: Dict[str, Any]) -> DeploymentConfig:
        if not content.get("environments"):
            dbx_echo(
                """[yellow bold]
                Since v0.7.0 environment configurations should be nested under [code]environments[/code] section.

                Please nest environment configurations under this section to avoid potential issues while using "build"
                configuration directive.[/yellow bold]
            """
            )
            return DeploymentConfig.from_legacy_json_payload(content)
        else:
            return DeploymentConfig.from_payload(content)


class Jinja2ConfigReader(_AbstractConfigReader):
    def __init__(self, path: Path, ext: str, jinja_vars_file: Optional[Path]):
        self._ext = ext
        self._jinja_vars_file = jinja_vars_file
        super().__init__(path)

    @staticmethod
    def _read_vars_file(file_path: Path) -> Dict[str, Any]:
        return yaml.load(file_path.read_text(encoding="utf-8"), yaml.SafeLoader)

    @classmethod
    def _render_content(cls, file_path: Path, _var: Dict[str, Any]) -> str:
        absolute_parent_path = file_path.absolute().parent
        file_name = file_path.name

        dbx_echo(f"The following path will be used for the jinja loader: {absolute_parent_path} with file {file_name}")

        env = jinja2.Environment(loader=jinja2.FileSystemLoader(absolute_parent_path))
        template = env.get_template(file_name)
        template.globals["dbx"] = dbx_jinja

        if CUSTOM_JINJA_FUNCTIONS_PATH.exists():
            cls.add_custom_functions(template)

        return template.render(env=os.environ, var=_var)

    @classmethod
    def add_custom_functions(cls, template: jinja2.Template):
        dbx_echo(f"🔌 Found custom Jinja functions defined in {CUSTOM_JINJA_FUNCTIONS_PATH}, loading them")
        _module = load_module_from_source(CUSTOM_JINJA_FUNCTIONS_PATH, "_custom")
        template.globals["custom"] = _module
        dbx_echo("✅ Custom Jinja functions successfully loaded")

    def _read_file(self) -> DeploymentConfig:
        _var = {} if not self._jinja_vars_file else self._read_vars_file(self._jinja_vars_file)
        rendered = self._render_content(self._path, _var)

        if self._ext == ".json":
            _content = json.loads(rendered)
            return JsonConfigReader.read_content(_content)
        elif self._ext in [".yml", ".yaml"]:
            _content = yaml.load(rendered, yaml.SafeLoader)
            return DeploymentConfig.from_payload(_content)
        else:
            raise Exception(f"Unexpected extension for Jinja reader: {self._ext}")


class ConfigReader:
    """
    Entrypoint for reading the raw configurations from files.
    In most cases there is no need to use the lower-level config readers.
    If a new reader is introduced, it shall be used via the :code:`_define_reader` method.
    """

    def __init__(self, path: Path, jinja_vars_file: Optional[Path] = None):
        self._jinja_vars_file = jinja_vars_file
        self._path = path
        self._reader = self._define_reader()

    def _define_reader(self) -> _AbstractConfigReader:
        if len(self._path.suffixes) > 1:
            if self._path.suffixes[0] in [".json", ".yaml", ".yml"] and self._path.suffixes[1] == ".j2":
                dbx_echo(
                    """[bright_magenta bold]You're using a deployment file with .j2 extension.
                    if you would like to use Jinja directly inside YAML or JSON files without changing the extension,
                    you can also configure your project to support in-place Jinja by running:
                    [code]dbx configure --enable-inplace-jinja-support[/code][/bright_magenta bold]"""
                )
                return Jinja2ConfigReader(self._path, ext=self._path.suffixes[0], jinja_vars_file=self._jinja_vars_file)
        elif ProjectConfigurationManager().get_jinja_support():
            return Jinja2ConfigReader(self._path, ext=self._path.suffixes[0], jinja_vars_file=self._jinja_vars_file)
        else:
            if self._jinja_vars_file:
                raise Exception(
                    "Jinja variables file is provided, but the deployment file is not based on Jinja "
                    "and inplace jinja support is not enabled for this project."
                )

            if self._path.suffixes[0] == ".json":
                return JsonConfigReader(self._path)
            elif self._path.suffixes[0] in [".yaml", ".yml"]:
                return YamlConfigReader(self._path)

        # no matching reader found, raising an exception
        raise Exception(
            f"Unexpected extension of the deployment file: {self._path}. "
            f"Please check the documentation for supported extensions."
        )

    def get_config(self) -> DeploymentConfig:
        return self._reader.config

    def get_environment(self, environment: str) -> Optional[EnvironmentDeploymentInfo]:
        """
        This method is mainly used for testing purposes
        """
        return self._reader.config.get_environment(environment)

    def get_all_environment_names(self) -> List[str]:
        return [e.name for e in self._reader.config.environments]
