import os
import re
import shutil

import click
import mlflow
from cookiecutter.main import cookiecutter
from databricks_cli.configure.config import get_profile_from_context
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.utils import CONTEXT_SETTINGS
from databricks_cli.workspace.api import WorkspaceService
from path import Path

import dbx
from dbx.cli.clusters import DEV_CLUSTER_FILE
from dbx.cli.utils import LockFile, InfoFile, update_json, dbx_echo, DATABRICKS_MLFLOW_URI

TEMPLATE_PATH = os.path.join(dbx.__path__[0], "template")
SPECS_PATH = os.path.join(dbx.__path__[0], "specs")


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Initializes a plain new project in a new directory.')
@click.option("--project-name", required=True, type=str, help="Name for a new project")
@click.option('--cloud', required=True, type=click.Choice(['Azure', 'AWS'], case_sensitive=True), help="Cloud provider")
@click.option('--pipeline-engine', required=True,
              type=click.Choice(['GitHub Actions', 'Azure Pipelines'], case_sensitive=True),
              help="Pipeline engine type")
@click.option("--override", is_flag=True, required=False, default=False, type=bool,
              help="Override existing project settings if directory exists")
@click.option('--dbx-workspace-dir', required=False, type=str,
              default="/dbx/projects",
              help='Base workspace directory for dbx projects')
@click.option('--dbx-artifact-location', required=False, type=str,
              default="dbfs:/dbx/projects",
              help='Base DBFS location for artifacts')
@profile_option
@provide_api_client
@debug_option
def init(api_client: ApiClient, **kwargs):
    """
    Initializes a plain new project in a new directory
    """
    verify_project_name(kwargs["project_name"])
    cookiecutter(TEMPLATE_PATH, extra_context=kwargs, no_input=True, overwrite_if_exists=kwargs["override"])

    dbx_echo("Initializing project in directory: %s" % os.path.join(os.getcwd(), kwargs["project_name"]))

    with Path(kwargs["project_name"]):
        create_dbx_files(**kwargs)
        prepare_dev_cluster(**kwargs)
        prepare_dbx_workspace_dir(api_client)
        initialize_artifact_storage(**kwargs)

        dbx_echo("Project initialization finished")


def initialize_artifact_storage(**kwargs):
    dbx_echo("Initializing artifact storage for the project")
    mlflow.set_tracking_uri("%s://%s" % (DATABRICKS_MLFLOW_URI, get_profile_from_context()))

    experiment_path = "%s/%s" % (InfoFile.get("dbx_workspace_dir"), kwargs["project_name"])

    if not experiment_exists(experiment_path):
        artifact_location = "%s/%s" % (InfoFile.get("dbx_artifact_location"), kwargs["project_name"])
        mlflow.create_experiment(experiment_path, artifact_location)
        dbx_echo("Project registered in dbx artifact storage")
    else:
        dbx_echo("Project is already registered in dbx artifact storage")

    mlflow.set_experiment(experiment_path)
    InfoFile.update({"experiment_path": experiment_path})


def prepare_dbx_workspace_dir(api_client):
    workspace_service = WorkspaceService(api_client)
    dbx_base_workspace_dir = InfoFile.get("dbx_workspace_dir")
    workspace_service.mkdirs(dbx_base_workspace_dir)


def create_dbx_files(**kwargs):
    dbx_echo("Creating dbx files")
    LockFile.initialize()
    InfoFile.initialize(kwargs)


def prepare_dev_cluster(**kwargs):
    dbx_echo("Preparing dev cluster specs")
    dev_cluster_name = "dev-%s-%s" % (kwargs["project_name"], LockFile.get("dbx_uuid"))

    dev_cluster_spec_path = os.path.join(SPECS_PATH, "config/dev/%s.json" % kwargs["cloud"].lower())
    shutil.copyfile(dev_cluster_spec_path, DEV_CLUSTER_FILE)

    update_json({"cluster_name": dev_cluster_name}, DEV_CLUSTER_FILE)


def experiment_exists(experiment_path):
    return mlflow.get_experiment_by_name(experiment_path)


def verify_project_name(project_name):
    name_regex = r'^[_a-zA-Z][_a-zA-Z0-9]+$'
    if not re.match(name_regex, project_name):
        raise ValueError("Project name %s is not a valid python package name" % project_name)
