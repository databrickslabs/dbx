import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Dict, Any

import jinja2
import yaml

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


class _YamlConfigReader(_AbstractConfigReader):
    def _read_file(self) -> DeploymentConfig:
        content = yaml.load(self._path.read_text(encoding="utf-8"), yaml.SafeLoader)
        _envs = content.get("environments")
        return DeploymentConfig.from_payload(_envs)


class _JsonConfigReader(_AbstractConfigReader):
    def _read_file(self) -> DeploymentConfig:
        _content = JsonUtils.read(self._path)
        return DeploymentConfig.from_payload(_content)


class _Jinja2ConfigReader(_AbstractConfigReader):
    def __init__(self, path: Path, ext: str, jinja_vars_file: Optional[Path]):
        self._ext = ext
        self._jinja_vars_file = jinja_vars_file
        super().__init__(path)

    @staticmethod
    def _read_vars_file(file_path: Path) -> Dict[str, Any]:
        return yaml.load(file_path.read_text(encoding="utf-8"), yaml.SafeLoader)

    def _read_file(self) -> DeploymentConfig:
        abs_parent_path = self._path.parent.absolute()
        file_name = self._path.name
        dbx_echo("Reading file as a Jinja2 template")
        dbx_echo(f"The following path will be used for the jinja loader: {abs_parent_path} with file {file_name}")
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(abs_parent_path))
        _var = {} if not self._jinja_vars_file else self._read_vars_file(self._jinja_vars_file)
        rendered = env.get_template(file_name).render(env=os.environ, var=_var)

        if self._ext == ".json":
            _content = json.loads(rendered)
        elif self._ext in [".yml", ".yaml"]:
            _content = yaml.load(rendered, yaml.SafeLoader).get("environments")
        else:
            raise Exception(f"Unexpected extension for Jinja reader: {self._ext}")

        return DeploymentConfig.from_payload(_content)


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
                return _Jinja2ConfigReader(
                    self._path, ext=self._path.suffixes[0], jinja_vars_file=self._jinja_vars_file
                )
        else:
            if self._jinja_vars_file:
                raise Exception("Jinja variables file is provided, but the deployment file is not based on Jinja.")

            if self._path.suffixes[0] == ".json":
                return _JsonConfigReader(self._path)
            elif self._path.suffixes[0] in [".yaml", ".yml"]:
                return _YamlConfigReader(self._path)

        # no matching reader found, raising an exception
        raise Exception(
            f"Unexpected extension of the deployment file: {self._path}. "
            f"Please check the documentation for supported extensions."
        )

    def get_environment(self, environment: str) -> Optional[EnvironmentDeploymentInfo]:
        return self._reader.config.get_environment(environment)

    def get_all_environment_names(self) -> List[str]:
        return [e.name for e in self._reader.config.environments]
