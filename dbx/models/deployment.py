from __future__ import annotations

from typing import Optional, Dict, Any, List
from pydantic import BaseModel, root_validator

from dbx.utils import dbx_echo


class Deployment(BaseModel):
    workflows: Optional[List[Dict[str, Any]]]

    @root_validator(pre=True)
    def check_inputs(cls, values: Dict[str, Any]):  # noqa
        if "jobs" in values:
            dbx_echo(
                ":warning: [red bold]Usage of jobs keyword is deprecated[/red bold]."
                "Please use [bold]workflows[bold] instead"
            )
        _w = values.get("jobs") if "jobs" in values else values.get("workflows")
        return {"workflows": _w}


class EnvironmentDeploymentInfo(BaseModel):
    name: str
    payload: Deployment

    def to_spec(self) -> Dict[str, Any]:
        _spec = {self.name: {"jobs": self.payload.workflows}}
        return _spec


class DeploymentConfig(BaseModel):
    environments: List[EnvironmentDeploymentInfo]

    def get_environment(self, name) -> Optional[EnvironmentDeploymentInfo]:
        _found = [env for env in self.environments if env.name == name]
        if len(_found) > 1:
            raise Exception(f"There are more than one environment with name {name} defined")
        if len(_found) == 0:
            return None
        return _found[0]

    @staticmethod
    def from_payload(payload: Dict[str, Any]) -> DeploymentConfig:
        _envs = []
        for name, _payload in payload.items():
            _env = EnvironmentDeploymentInfo(name=name, payload=_payload)
            _envs.append(_env)
        return DeploymentConfig(environments=_envs)
