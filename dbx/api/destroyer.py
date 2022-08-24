from __future__ import annotations

from typing import Optional, List, Callable

import mlflow
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk import ApiClient
from mlflow.entities import Run
from pydantic import BaseModel
from pydantic import root_validator
from rich.progress import track
from typer.rich_utils import _get_rich_console  # noqa

from dbx.api.configure import ProjectConfigurationManager
from dbx.models.deployment import EnvironmentDeploymentInfo
from dbx.utils import dbx_echo


class DestroyerConfig(BaseModel):
    workflows: Optional[List[str]]
    workflows_only: bool
    assets_only: bool
    dry_run: Optional[bool] = False
    dracarys: Optional[bool] = False
    deployment: EnvironmentDeploymentInfo

    @root_validator()
    def validate_all(cls, values):  # noqa
        _dc = values["deployment"]
        if not values["workflows"]:
            dbx_echo("No workflows were specified, all workflows will be taken from deployment file")
            values["workflows"] = [w["name"] for w in _dc.payload.workflows]
        else:
            _ws_names = [w["name"] for w in _dc.payload.workflows]
            for w in values["workflows"]:
                if w not in _ws_names:
                    raise ValueError(f"Workflow name {w} not found in {_ws_names}")
        return values


class Destroyer:
    def __init__(self, api_client: ApiClient, config: DestroyerConfig):
        self._client = api_client
        self._config = config

    def _define_actions(self) -> List[Callable]:
        actions = []

        if self._config.dracarys:
            actions.append(self._dracarys_start)
        if self._config.workflows_only:
            actions.append(self._delete_workflows)
        elif self._config.assets_only:
            actions.append(self._delete_assets)
        else:
            actions.append(self._delete_workflows)
            actions.append(self._delete_assets)

        return actions

    def launch(self):
        dbx_echo("🚮 Launching the destroy process")
        actions = self._define_actions()

        for action in actions:
            action()

        if self._config.dracarys:
            self._dracarys_finish()
        else:
            dbx_echo("Destroy process finished ✅")

    def _delete_workflow(self, workflow):
        dbx_echo(f"Job object {workflow} will be deleted")
        api = JobsApi(self._client)
        found = api._list_jobs_by_name(workflow)  # noqa

        if len(found) > 1:
            raise Exception(f"More than one job with name {workflow} was found, please check the duplicates in the UI")
        if len(found) == 0:
            dbx_echo(f"Job with name {workflow} already doesn't exist")
        else:
            _job = found[0]
            if self._config.dry_run:
                dbx_echo(f"Job {workflow} with definition {_job} would be deleted in case of a real run")
            else:
                api.delete_job(_job["job_id"])
                dbx_echo(f"Job object with name {workflow} was successfully deleted ✅")

    def _delete_assets(self):
        dbx_echo("Deleting the assets")
        proj_config = ProjectConfigurationManager().get(self._config.deployment.name)
        experiment = mlflow.get_experiment_by_name(proj_config.properties.workspace_directory)
        _runs: List[Run] = mlflow.search_runs(experiment_ids=[experiment.experiment_id], output_format="list")
        description = (
            "Listing assets in the artifact storage"
            if self._config.dry_run
            else "Deleting assets in the artifact storage"
        )
        for run in track(_runs, description=description):
            artifact_id = run.info.run_id
            if self._config.dry_run:
                dbx_echo(f"Artifact with id {artifact_id} would be deleted in case of a real run")
            else:
                mlflow.delete_run(run.info.run_id)
        dbx_echo("Assets deletion finished successfully ✅")

    def _delete_workflows(self):
        for w in self._config.workflows:
            self._delete_workflow(w)

    def _dracarys_start(self):
        if self._config.dry_run:
            dbx_echo("Huge message would be displayed here if it was a real run")
        else:
            _console = _get_rich_console()
            fires = "\n".join(["🔥" * _console.width for _ in range(2)])
            _console.print("[red bold]DRACARYS[/red bold]", justify="center")
            _console.print(fires)

    def _dracarys_finish(self):
        if self._config.dry_run:
            dbx_echo("Huge message would be displayed here if it was a real run")
        else:
            _console = _get_rich_console()
            fires = "\n".join(["🔥" * _console.width for _ in range(2)])
            _console.print(fires)
