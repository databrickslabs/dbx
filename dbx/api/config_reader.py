import json
import os
from pathlib import Path
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any

import jinja2
import yaml

from dbx.utils import dbx_echo
from dbx.utils.json import JsonUtils


class _AbstractConfigReader(ABC):
    def __init__(self, path: Path):
        self._path = path
        self.config = self._get_config()

    def _get_config(self) -> Dict[str, Any]:
        return self._read_file()

    @abstractmethod
    def _read_file(self) -> Dict[str, Any]:
        """"""


class _YamlConfigReader(_AbstractConfigReader):
    def _read_file(self) -> Dict[str, Any]:
        content = yaml.load(self._path.read_text(encoding="utf-8"), yaml.SafeLoader)
        return content.get("environments")


class _JsonConfigReader(_AbstractConfigReader):
    def _read_file(self) -> Dict[str, Any]:
        return JsonUtils.read(self._path)


class _Jinja2ConfigReader(_AbstractConfigReader):
    def __init__(self, path: Path, ext: str, jinja_vars_file: Optional[Path]):
        self._ext = ext
        self._jinja_vars_file = jinja_vars_file
        super().__init__(path)

    @staticmethod
    def _read_vars_file(file_path: Path) -> Dict[str, Any]:
        return yaml.load(file_path.read_text(encoding="utf-8"), yaml.SafeLoader)

    def _read_file(self) -> Dict[str, Any]:
        abs_parent_path = self._path.parent.absolute()
        file_name = self._path.name
        dbx_echo("Reading file as a Jinja2 template")
        dbx_echo(f"The following path will be used for the jinja loader: {abs_parent_path} with file {file_name}")
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(abs_parent_path))
        _var = {} if not self._jinja_vars_file else self._read_vars_file(self._jinja_vars_file)
        rendered = env.get_template(file_name).render(env=os.environ, var=_var)
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

    def __init__(self, path: Optional[Path] = None, jinja_vars_file: Optional[Path] = None):
        self._jinja_vars_file = jinja_vars_file
        self._path = self._verify_deployment_file(path) if path else self._find_deployment_file()
        self._reader = self._define_reader()

    @staticmethod
    def _verify_deployment_file(candidate: Path) -> Path:
        file_extension = candidate.suffixes[-1]

        if file_extension == ".j2":
            file_extension = candidate.suffixes[-2]
        if file_extension not in [".json", ".yaml", ".yml"]:
            raise Exception(
                "Deployment file should have one of these extensions:"
                '[".json", ".yaml", ".yml", "json.j2", "yaml.j2", "yml.j2"]'
            )

        if not candidate.exists():
            raise Exception(f"Deployment file {candidate} does not exist")

        dbx_echo(f"Using the provided deployment file {candidate}")
        return candidate

    @staticmethod
    def _find_deployment_file() -> Path:
        potential_extensions = ["json", "yml", "yaml", "json.j2", "yaml.j2", "yml.j2"]

        for ext in potential_extensions:
            candidate = Path(f"conf/deployment.{ext}")
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

    def get_environment(self, environment: str) -> Optional[Dict[str, Any]]:
        return self._reader.config.get(environment)

    def get_all_environment_names(self) -> List[str]:
        return list(self._reader.config.keys())
