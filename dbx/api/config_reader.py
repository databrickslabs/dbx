import json
import os
import pathlib
from abc import ABC, abstractmethod
from typing import List, Optional

import jinja2
import yaml

from dbx.models.deployment import Deployment, Environment
from dbx.utils.json import JsonUtils


class _AbstractConfigReader(ABC):
    def __init__(self, path: pathlib.Path):
        self._path = path
        self.config = self._read_file()

    @abstractmethod
    def _read_file(self) -> Deployment:
        pass

    def _get_file_extensions(self) -> List[str]:
        return self._path.suffixes


class _YamlConfigReader(_AbstractConfigReader):
    def _read_file(self) -> Deployment:
        content = yaml.load(self._path.read_text(encoding="utf-8"), yaml.SafeLoader)
        return Deployment(**content)


class _JsonConfigReader(_AbstractConfigReader):
    def _read_file(self) -> Deployment:
        return Deployment(**JsonUtils.read(self._path))


class _Jinja2ConfigReader(_AbstractConfigReader):
    def __init__(self, path: pathlib.Path, ext: str):
        self._ext = ext
        super().__init__(path)

    def _read_file(self) -> Deployment:
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(self._path.parent))
        rendered = env.get_template(str(self._path)).render(os=os.environ)
        if self._ext == ".json":
            return json.loads(rendered)
        elif self._ext in [".yml", ".yaml"]:
            return yaml.load(rendered, yaml.SafeLoader).get("environments")


class ConfigReader:
    """
    Entrypoint for reading the raw configurations from files.
    In most cases there is no need to use the lower-level config readers.
    If a new reader is introduced, it shall be used via the :code:`_define_reader` method.
    """

    def __init__(self, path: pathlib.Path):
        self._path = path
        self._reader = self._define_reader()

    def _define_reader(self) -> _AbstractConfigReader:
        if len(self._path.suffixes) > 1:
            if self._path.suffixes[0] in [".json", ".yaml", ".yml"] and self._path.suffixes[1] == ".j2":
                return _Jinja2ConfigReader(self._path, ext=self._path.suffixes[0])
        else:
            if self._path.suffixes[0] == ".json":
                return _JsonConfigReader(self._path)
            elif self._path.suffixes[0] in [".yaml", ".yml"]:
                return _YamlConfigReader(self._path)

        # no matching reader found, raising an exception
        raise Exception(
            f"Unexpected extension of the deployment file: {self._path}. "
            f"Please check the documentation for supported extensions."
        )

    def get_environment(self, environment: str) -> Optional[Environment]:
        return self._reader.config.environments.get(environment)

    def get_all_environment_names(self) -> List[str]:
        return list(self._reader.config.environments.keys())
