import json
import os
import pathlib
from abc import ABC, abstractmethod
from typing import List, Optional

import jinja2
import yaml

from dbx.models.deployment import Deployments, Environment
from dbx.utils.json import JsonUtils


class AbstractConfigReader(ABC):
    def __init__(self, path: pathlib.Path):
        self._path = path
        self.config = self._read_file()

    @abstractmethod
    def _read_file(self) -> Deployments:
        pass

    def _get_file_extensions(self) -> List[str]:
        return self._path.suffixes


class YamlConfigReader(AbstractConfigReader):
    def _read_file(self) -> Deployments:
        content = yaml.load(self._path.read_text(encoding="utf-8"), yaml.SafeLoader)
        return Deployments(**content)


class JsonConfigReader(AbstractConfigReader):
    def _read_file(self) -> Deployments:
        return Deployments(**JsonUtils.read(self._path))


class Jinja2ConfigReader(AbstractConfigReader):
    def _read_file(self) -> Deployments:
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(self._path.parent))
        rendered = env.get_template(str(self._path)).render(os=os.environ)
        ext = self._get_file_extensions()[0]
        if ext == "json":
            return json.loads(rendered)
        elif ext in ["yml", "yaml"]:
            return yaml.load(rendered, yaml.SafeLoader).get("environments")
        else:
            raise Exception(f"File extension {ext} is not supported for Jinja2 template reader")


class ConfigProvider:
    def __init__(self, path: pathlib.Path):
        self._path = path
        self._reader = self._define_reader()

    def _define_reader(self) -> AbstractConfigReader:
        if len(self._path.suffixes) > 1:
            if self._path.suffixes[0] in [".json", ".yaml", ".yml"] and self._path.suffixes[1] == ".j2":
                return Jinja2ConfigReader(self._path)
        else:
            if self._path.suffixes[0] == ".json":
                return JsonConfigReader(self._path)
            elif self._path.suffixes[0] in [".yaml", ".yml"]:
                return YamlConfigReader(self._path)

        # no matching reader found, raising an exception
        raise Exception(
            f"Unexpected extension of the deployment file: {self._path}. "
            f"Please check the documentation for supported extensions."
        )

    def get_environment(self, environment: str) -> Optional[Environment]:
        return self._reader.config.environments.get(environment)

    def get_all_environment_names(self) -> List[str]:
        return list(self._reader.config.environments.keys())
