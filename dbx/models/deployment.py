from __future__ import annotations

from copy import deepcopy
from enum import Enum
from typing import Optional, Dict, Any, List

from pydantic import BaseModel, root_validator, validator

from dbx.api.configure import ProjectConfigurationManager
from dbx.models.project import EnvironmentInfo
from dbx.utils import dbx_echo


class Deployment(BaseModel):
    workflows: Optional[List[Dict[str, Any]]]

    @root_validator(pre=True)
    def check_inputs(cls, values: Dict[str, Any]):  # noqa
        if "jobs" in values:
            dbx_echo(
                "[yellow bold]Usage of jobs keyword in deployment file is deprecated. "
                "Please use [bold]workflows[bold] instead (simply rename this section to workflows).[/yellow bold]"
            )
        _w = values.get("jobs") if "jobs" in values else values.get("workflows")
        return {"workflows": _w}


class PythonBuild(str, Enum):
    pip = "pip"
    poetry = "poetry"
    flit = "flit"


class BuildConfiguration(BaseModel):
    no_build: Optional[bool] = False
    commands: Optional[List[str]] = []
    python: Optional[PythonBuild]

    @root_validator(pre=True)
    def init_default(cls, values):  # noqa
        _v = values if values else {"python": "pip"}
        return _v


class EnvironmentDeploymentInfo(BaseModel):
    name: str
    payload: Deployment

    def to_spec(self) -> Dict[str, Any]:
        _spec = {self.name: {"jobs": self.payload.workflows}}
        return _spec

    def get_project_info(self) -> EnvironmentInfo:
        """
        Convenience method for cases when the project information about specific environment is required.
        """
        return ProjectConfigurationManager().get(self.name)


class DeploymentConfig(BaseModel):
    environments: List[EnvironmentDeploymentInfo]
    build: Optional[BuildConfiguration]

    @validator("build", pre=True)
    def default_build(cls, value):  # noqa
        build_spec = value if value else {"python": "pip"}
        return build_spec

    def get_environment(self, name, raise_if_not_found: Optional[bool] = False) -> Optional[EnvironmentDeploymentInfo]:
        _found = [env for env in self.environments if env.name == name]
        if len(_found) > 1:
            raise Exception(f"More than one environment with name {name} is defined in the project file")
        if len(_found) == 0:
            if raise_if_not_found:
                raise NameError(
                    f"""
                    Environment {name} not found in the deployment file.
                    Available environments are: {[e.name for e in self.environments]}
                """
                )
            return None

        return _found[0]

    @staticmethod
    def prepare_build(payload: Dict[str, Any]) -> BuildConfiguration:
        _build_payload = payload.get("build", {})
        if not _build_payload:
            dbx_echo(
                "No build logic defined in the deployment file. "
                "Default [code]pip[/code]-based build logic will be used."
            )
        return BuildConfiguration(**_build_payload)

    @classmethod
    def from_legacy_json_payload(cls, payload: Dict[str, Any]) -> DeploymentConfig:

        _build = cls.prepare_build(payload)

        _envs = []
        for name, _env_payload in payload.items():
            if name == "build":
                raise ValueError(
                    """Deployment file with a legacy syntax uses "build" as an environment name.
                This behaviour is not supported since dbx v0.7.0.
                Please nest all environment configurations under "environments" key in the deployment file."""
                )
            _env = EnvironmentDeploymentInfo(name=name, payload=_env_payload)
            _envs.append(_env)

        return DeploymentConfig(environments=_envs, build=_build)

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> DeploymentConfig:
        _payload = deepcopy(payload)
        _envs = [
            EnvironmentDeploymentInfo(name=name, payload=env_payload)
            for name, env_payload in _payload.get("environments", {}).items()
        ]
        _build = cls.prepare_build(_payload)
        return DeploymentConfig(environments=_envs, build=_build)
