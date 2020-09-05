import logging
import os
from pathlib import Path

import click
import mlflow
from databricks_cli.configure.config import debug_option, profile_option
from databricks_cli.configure.config import get_profile_from_context
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.utils import CONTEXT_SETTINGS
from databricks_cli.workspace.api import WorkspaceService
from requests.exceptions import HTTPError

from dbx.cli.utils import InfoFile, dbx_echo, DATABRICKS_MLFLOW_URI, INFO_FILE_NAME, _get_api_client


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Configures new environment.')
@click.option("--name", required=True, type=str,
              help="Environment name.")
@click.option("--workspace-dir", required=True, type=str,
              help="Workspace directory for MLflow experiment.")
@click.option("--artifact-location", required=False, type=str,
              help="DBFS path to a custom artifact location.")
@debug_option
@profile_option
def configure(
        name: str,
        workspace_dir: str,
        artifact_location: str):
    dbx_echo("Configuring new environment with name %s" % name)

    if not os.path.exists(INFO_FILE_NAME):
        InfoFile.initialize({"environments": {}})

    if InfoFile.get("environments").get(name):
        raise Exception("Environment with name %s already exists" % name)

    profile = get_profile_from_context()
    api_client = _get_api_client(profile)

    create_workspace_dir(api_client, workspace_dir)
    experiment_data = initialize_artifact_storage(workspace_dir, artifact_location)

    environment_info = {
        name: {
            "profile": get_profile_from_context(),
            "workspace_dir": workspace_dir,
            "experiment_id": experiment_data.experiment_id,
            "artifact_location": experiment_data.artifact_location
        }
    }

    environments = InfoFile.get("environments")

    environments.update(environment_info)

    InfoFile.update({"environments": environments})


def create_workspace_dir(api_client: ApiClient, path: str):
    workspace_service = WorkspaceService(api_client)
    parent_path = str(Path(path).parent)
    try:
        workspace_service.get_status(parent_path)
    except HTTPError:
        workspace_service.mkdirs(parent_path)


def initialize_artifact_storage(workspace_dir: str, artifact_location: str):
    dbx_echo("Initializing artifact storage for the project with workspace dir %s" % workspace_dir)
    tracking_uri = "%s://%s" % (DATABRICKS_MLFLOW_URI, get_profile_from_context())
    mlflow.set_tracking_uri(tracking_uri)
    logging.debug("Tracking uri is set to %s" % tracking_uri)

    if not mlflow.get_experiment_by_name(workspace_dir):
        mlflow.create_experiment(workspace_dir, artifact_location)
        dbx_echo("Project registered in dbx artifact storage")
    else:
        dbx_echo("Project is already registered in dbx artifact storage")

    experiment_data = mlflow.get_experiment_by_name(workspace_dir)
    return experiment_data
