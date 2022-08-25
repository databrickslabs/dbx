from __future__ import annotations

import time
from typing import List, Callable

import mlflow
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk import ApiClient
from mlflow.entities import Run
from rich.progress import track
from typer.rich_utils import _get_rich_console  # noqa

from dbx.api.configure import ProjectConfigurationManager
from dbx.models.destroyer import DestroyerConfig, DeletionMode
from dbx.utils import dbx_echo


class Destroyer:
    def __init__(self, api_client: ApiClient, config: DestroyerConfig):
        self._client = api_client
        self._config = config

    def _define_actions(self) -> List[Callable]:
        actions = []

        if self._config.dracarys:
            actions.append(self._dracarys_start)
        if self._config.deletion_mode == DeletionMode.workflows_only:
            actions.append(self._delete_workflows)
        elif self._config.deletion_mode == DeletionMode.assets_only:
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
        if _runs:
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
                    time.sleep(0.01)  # not to overflow the MLflow API
                    mlflow.delete_run(run.info.run_id)

            dbx_echo(f"Total {len(_runs)} artifact versions were deleted")
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
