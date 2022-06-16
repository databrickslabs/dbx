import json
import os
import pathlib
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any

import jinja2
import yaml

from dbx.utils.json import JsonUtils
from dbx.utils. import dbx_echo


class _AbstractConfigReader(ABC):
    def __init__(self, path: pathlib.Path):
        self._path = path
        self.config = self._read_file()

    @abstractmethod
    def _read_file(self) -> Dict[str, Any]:
        pass

    def _get_file_extensions(self) -> List[str]:
        return self._path.suffixes


class _YamlConfigReader(_AbstractConfigReader):
    def _read_file(self) -> Dict[str, Any]:
        content = yaml.load(self._path.read_text(encoding="utf-8"), yaml.SafeLoader)
        return content.get("environments")


class _JsonConfigReader(_AbstractConfigReader):
    def _read_file(self) -> Dict[str, Any]:
        return JsonUtils.read(self._path)


class _Jinja2ConfigReader(_AbstractConfigReader):
    def __init__(self, path: pathlib.Path, ext: str):
        self._ext = ext
        super().__init__(path)

    def _read_file(self) -> Dict[str, Any]:
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(self._path.parent))
        rendered = env.get_template(self._path.name).render(env=os.environ)
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

    def __init__(self, path: Optional[pathlib.Path] = None):
        self._path = self._verify_deployment_file(path) if path else self._find_deployment_file()
        self._reader = self._define_reader()

    @staticmethod
    def _verify_deployment_file(candidate: pathlib.Path) -> pathlib.Path:
        file_extension = candidate.suffixes[-1]

        if file_extension == "j2":
            file_extension = candidate.suffixes[-2]
        if file_extension not in ["json", "yaml", "yml"]:
            raise Exception(
                "Deployment file should have one of these extensions:"
                '[".json", ".yaml", ".yml", "json.j2", "yaml.j2", "yml.j2"]'
            )

        if not candidate.exists():
            raise Exception(f"Deployment file {candidate} does not exist")

        dbx_echo(f"Using the provided deployment file {candidate}")
        return candidate

    @staticmethod
    def _find_deployment_file() -> pathlib.Path:
        potential_extensions = ["json", "yml", "yaml", "json.j2", "yaml.j2", "yml.j2"]

        for ext in potential_extensions:
            candidate = pathlib.Path(f"conf/deployment.{ext}")
            if candidate.exists():
                dbx_echo(f"Auto-discovery found deployment file {candidate}")
                return candidate

        raise Exception(
            "Auto-discovery was unable to find any deployment file in the conf directory. "
            "Please provide file name via --deployment-file option"
        )

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

    def get_environment(self, environment: str) -> Optional[Dict[str, Any]]:
        return self._reader.config.get(environment)

    def get_all_environment_names(self) -> List[str]:
        return list(self._reader.config.keys())
