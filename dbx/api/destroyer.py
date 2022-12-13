from __future__ import annotations

import inspect
import time
from abc import ABC, abstractmethod
from typing import List

import mlflow
from databricks_cli.sdk import ApiClient
from mlflow.entities import Run
from rich.markup import escape
from rich.progress import track
from typer.rich_utils import _get_rich_console  # noqa

from dbx.api.services.jobs import NamedJobsService
from dbx.api.services.pipelines import NamedPipelinesService
from dbx.models.cli.destroyer import DestroyerConfig, DeletionMode
from dbx.models.deployment import AnyWorkflow
from dbx.models.files.project import EnvironmentInfo
from dbx.models.workflow.common.workflow_types import WorkflowType
from dbx.utils import dbx_echo


class Eraser(ABC):
    @abstractmethod
    def erase(self):
        """"""


class WorkflowEraser(Eraser):
    def __init__(self, api_client: ApiClient, workflows: List[AnyWorkflow], dry_run: bool):
        self._client = api_client
        self._workflows = workflows
        self._dry_run = dry_run
        self._pipeline_service = NamedPipelinesService(self._client)
        self._jobs_service = NamedJobsService(self._client)

    def _delete_workflow(self, workflow: AnyWorkflow):
        dbx_echo(f"Workflow {escape(workflow.name)} will be deleted")
        service_instance = (
            self._jobs_service if not workflow.workflow_type == WorkflowType.pipeline else self._pipeline_service
        )
        obj_id = service_instance.find_by_name(workflow.name)

        if not obj_id:
            dbx_echo(f"Workflow with name {escape(workflow.name)} doesn't exist, no deletion is required")
        else:
            if self._dry_run:
                dbx_echo(f"Workflow {escape(workflow.name)} with definition would be deleted in case of a real run")
            else:
                service_instance.delete(obj_id)
                dbx_echo(f"Workflow object with name {escape(workflow.name)} was successfully deleted ✅")

    def erase(self):
        for w in self._workflows:
            self._delete_workflow(w)


class AssetEraser(Eraser):
    def __init__(self, environment_info: EnvironmentInfo, dry_run: bool):
        self._env = environment_info
        self._dry_run = dry_run

    def __delete_found_assets(self, _runs: List[Run]):
        description = (
            "Listing assets in the artifact storage" if self._dry_run else "Deleting assets in the artifact storage"
        )
        for run in track(_runs, description=description):
            artifact_id = run.info.run_id
            if self._dry_run:
                dbx_echo(f"Artifact with id {artifact_id} would be deleted in case of a real run")
            else:
                time.sleep(0.01)  # not to overflow the MLflow API
                mlflow.delete_run(run.info.run_id)
        dbx_echo(f"Total {len(_runs)} artifact versions were deleted")

    def erase(self):
        dbx_echo("Deleting the assets")
        w_dir = self._env.properties.workspace_directory
        experiment = mlflow.get_experiment_by_name(w_dir)

        if not experiment:
            dbx_echo(
                inspect.cleandoc(
                    f"""The artifact storage is based on a non-existent mlflow experiment.
                This experiment was expected to be stored in Workspace directory {w_dir}.
                Therefore there are no assets to be deleted."""
                )
            )
        else:
            _runs: List[Run] = mlflow.search_runs(experiment_ids=[experiment.experiment_id], output_format="list")
            if _runs:
                self.__delete_found_assets(_runs)
            dbx_echo("Assets deletion finished successfully ✅")


class DracarysPrinter:
    def __init__(self, dry_run: bool):
        self._dry_run = dry_run

    def __enter__(self):
        if self._dry_run:
            dbx_echo("Huge message would be displayed here if it was a real run")
        else:
            _console = _get_rich_console()
            fires = "\n".join(["🔥" * _console.width for _ in range(2)])
            _console.print("[red bold]DRACARYS[/red bold]", justify="center")
            _console.print(fires)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._dry_run:
            dbx_echo("Huge message would be displayed here if it was a real run")
        else:
            _console = _get_rich_console()
            fires = "\n".join(["🔥" * _console.width for _ in range(2)])
            _console.print(fires)


class NormalPrinter:
    def __enter__(self):
        dbx_echo("🚮 Launching the destroy process")

    def __exit__(self, exc_type, exc_val, exc_tb):
        dbx_echo("Destroy process finished ✅")


class Destroyer:
    def __init__(self, api_client: ApiClient, config: DestroyerConfig):
        self._client = api_client
        self._config = config

    def _get_workflow_eraser(self) -> WorkflowEraser:
        return WorkflowEraser(self._client, self._config.workflows, self._config.dry_run)

    def _get_asset_eraser(self) -> AssetEraser:
        env_info = self._config.deployment.get_project_info()
        return AssetEraser(env_info, dry_run=self._config.dry_run)

    def _get_erasers(self) -> List[Eraser]:
        _erasers = []

        if self._config.deletion_mode == DeletionMode.workflows_only:
            _erasers.append(self._get_workflow_eraser())
        elif self._config.deletion_mode == DeletionMode.assets_only:
            _erasers.append(self._get_asset_eraser())
        else:
            _erasers.append(self._get_workflow_eraser())
            _erasers.append(self._get_asset_eraser())

        return _erasers

    def launch(self):

        printer = DracarysPrinter(self._config.dry_run) if self._config.dracarys else NormalPrinter()

        with printer:
            for _eraser in self._get_erasers():
                _eraser.erase()
