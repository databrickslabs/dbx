from __future__ import annotations

import collections
from typing import Optional, Dict, Any, List, Union

from pydantic import BaseModel, validator, Field
from rich.markup import escape
from typing_extensions import Annotated

from dbx.api.configure import ProjectConfigurationManager
from dbx.models.build import BuildConfiguration
from dbx.models.files.project import EnvironmentInfo
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.pipeline import Pipeline
from dbx.models.workflow.common.workflow_types import WorkflowType
from dbx.models.workflow.v2dot0.workflow import Workflow as V2dot0Workflow
from dbx.models.workflow.v2dot1.workflow import Workflow as V2dot1Workflow
from dbx.utils import dbx_echo

AnyWorkflow = Annotated[Union[V2dot0Workflow, V2dot1Workflow, Pipeline], Field(discriminator="workflow_type")]
WorkflowList = List[AnyWorkflow]


class WorkflowListMixin(BaseModel):
    workflows: Optional[WorkflowList]

    @property
    def workflow_names(self) -> List[str]:
        return [w.name for w in self.workflows]

    @validator("workflows")
    def _validate_unique(cls, workflows: Optional[WorkflowList]):  # noqa
        if workflows:
            _duplicates = [
                name for name, count in collections.Counter([w.name for w in workflows]).items() if count > 1
            ]
            if _duplicates:
                raise ValueError(f"Duplicated workflow names: {_duplicates}")
            return workflows
        else:
            return []

    def get_workflow(self, name) -> AnyWorkflow:
        _found = list(filter(lambda w: w.name == name, self.workflows))
        if not _found:
            raise ValueError(f"Workflow {name} not found. Available workflows are {self.workflow_names}")
        return _found[0]


class Deployment(FlexibleModel, WorkflowListMixin):
    @staticmethod
    def from_spec_local(raw_spec: Dict[str, Any]) -> Deployment:
        if "jobs" in raw_spec:
            dbx_echo(
                "[yellow bold]Usage of jobs keyword in deployment file is deprecated. "
                "Please use [bold]workflows[bold] instead (simply rename this section to workflows).[/yellow bold]"
            )
        return Deployment.from_spec_remote(raw_spec)

    @staticmethod
    def from_spec_remote(raw_spec: Dict[str, Any]) -> Deployment:
        _wfs = raw_spec.get("jobs") if "jobs" in raw_spec else raw_spec.get("workflows")
        assert isinstance(_wfs, list), ValueError(f"Provided payload is not a list {_wfs}")

        for workflow_def in _wfs:
            if not workflow_def.get("workflow_type"):
                workflow_def["workflow_type"] = (
                    WorkflowType.job_v2d1 if "tasks" in workflow_def else WorkflowType.job_v2d0
                )
        return Deployment(**{"workflows": _wfs})

    def select_relevant_or_all_workflows(
        self, workflow_name: Optional[str] = None, workflow_names: Optional[List[str]] = None
    ) -> WorkflowList:

        if workflow_name and workflow_names:
            raise Exception("Workflow argument and --workflows (or --job and --jobs) cannot be provided together")

        if workflow_name:
            dbx_echo(f"The workflow {escape(workflow_name)} was selected for further operations")
            return [self.get_workflow(workflow_name)]
        elif workflow_names:
            dbx_echo(f"Workflows {[escape(w) for w in workflow_names]} were selected for further operations")
            return [self.get_workflow(w) for w in workflow_names]
        else:
            dbx_echo(
                f"All available workflows were selected for further operations: "
                f"{[escape(w) for w in self.workflow_names]}"
            )
            return self.workflows


class EnvironmentDeploymentInfo(BaseModel):
    name: str
    payload: Deployment

    def to_spec(self) -> Dict[str, Any]:
        _spec = {self.name: self.payload.dict(exclude_none=True)}
        return _spec

    @staticmethod
    def from_spec(
        environment_name: str, raw_spec: Dict[str, Any], reader_type: Optional[str] = "local"
    ) -> EnvironmentDeploymentInfo:
        deployment_reader = Deployment.from_spec_local if reader_type == "local" else Deployment.from_spec_remote
        if not raw_spec:
            raise ValueError(f"Deployment result for {environment_name} doesn't contain any workflow definitions")

        _spec = {"name": environment_name, "payload": deployment_reader(raw_spec)}

        return EnvironmentDeploymentInfo(**_spec)

    def get_project_info(self) -> EnvironmentInfo:
        """
        Convenience method for cases when the project information about specific environment is required.
        """
        return ProjectConfigurationManager().get(self.name)


class DeploymentConfig(BaseModel):
    environments: List[EnvironmentDeploymentInfo]
    build: Optional[BuildConfiguration] = BuildConfiguration()

    @staticmethod
    def _prepare_build(payload: Dict[str, Any]) -> BuildConfiguration:
        _build_payload = payload.get("build", {})
        if not _build_payload:
            dbx_echo(
                "No build logic defined in the deployment file. "
                "Default [code]pip[/code]-based build logic will be used."
            )
        return BuildConfiguration(**_build_payload)

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

    @classmethod
    def from_legacy_json_payload(cls, payload: Dict[str, Any]) -> DeploymentConfig:

        _build = cls._prepare_build(payload)

        _envs = []
        for name, _env_payload in payload.items():
            if name == "build":
                raise ValueError(
                    """Deployment file with a legacy syntax uses "build" as an environment name.
                This behaviour is not supported since dbx v0.7.0.
                Please nest all environment configurations under "environments" key in the deployment file."""
                )
            _env = EnvironmentDeploymentInfo.from_spec(name, _env_payload)
            _envs.append(_env)

        return DeploymentConfig(environments=_envs, build=_build)

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> DeploymentConfig:
        _env_payloads = payload.get("environments", {})
        _envs = [EnvironmentDeploymentInfo.from_spec(name, env_payload) for name, env_payload in _env_payloads.items()]
        _build = cls._prepare_build(payload)
        return DeploymentConfig(environments=_envs, build=_build)
