import os
import pathlib
import re
import shutil
import subprocess

import click
import jinja2
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
               short_help='Initializes a plain new project in a new directory')
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
@click.option("--no-sample-job", is_flag=True, required=False, default=False, type=bool,
              help="Disable sample job generation during project init")
@profile_option
@provide_api_client
@debug_option
def init(api_client: ApiClient, override, no_sample_job, **kwargs):
    """
    Initializes a plain new project in a new directory
    """
    verify_project_name(kwargs["project_name"])
    cookiecutter(TEMPLATE_PATH, extra_context=kwargs, no_input=True, overwrite_if_exists=override)

    dbx_echo("Initializing project in directory: %s" % os.path.join(os.getcwd(), kwargs["project_name"]))

    with Path(kwargs["project_name"]):
        create_dbx_files(**kwargs)
        postprocess_project_dir(**kwargs)

        if not no_sample_job:
            create_job("sample", **kwargs)

        prepare_dbx_workspace_dir(api_client)
        initialize_artifact_storage(**kwargs)

        dbx_echo("Project initialization finished")


def create_job(job_name, environments=None, **kwargs):
    if environments is None:
        environments = ["test"]

    verify_job_name(job_name)

    code_dir = os.path.join(kwargs["project_name"], "jobs", job_name)
    os.makedirs(code_dir)

    pathlib.Path(os.path.join(code_dir, "__init__.py")).touch()

    job_code = prepare_entrypoint_template(kwargs["project_name"])
    pathlib.Path(os.path.join(code_dir, "entrypoint.py")).write_text(job_code)

    test_code = prepare_test_template(kwargs["project_name"])
    pathlib.Path(os.path.join("tests", "test_sample.py")).write_text(test_code)

    for env in environments:
        conf_dir = os.path.join("config", env, job_name)
        os.makedirs(conf_dir)

        launch_config_path = os.path.join(SPECS_PATH, "job", "launch", "%s.json" % kwargs["cloud"].lower())
        shutil.copyfile(launch_config_path, os.path.join(conf_dir, "launch.json"))

        arguments_config_path = os.path.join(SPECS_PATH, "job", "arguments.json")
        shutil.copyfile(arguments_config_path, os.path.join(conf_dir, "arguments.json"))


def prepare_entrypoint_template(project_name):
    template_loader = jinja2.FileSystemLoader(searchpath=os.path.join(SPECS_PATH, "job"))
    template_env = jinja2.Environment(loader=template_loader)
    template = template_env.get_template("entrypoint.py.template")
    content = template.render(project_name=project_name)
    return content


def prepare_test_template(project_name):
    template_loader = jinja2.FileSystemLoader(searchpath=os.path.join(SPECS_PATH, "job"))
    template_env = jinja2.Environment(loader=template_loader)
    template = template_env.get_template("test_sample.py.template")
    content = template.render(project_name=project_name)
    return content


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
    profile = get_profile_from_context()
    if profile:
        InfoFile.update({"profile": profile})
    else:
        InfoFile.update({"profile": "DEFAULT"})  # in case if profile wasn't provided


def postprocess_project_dir(**kwargs):
    dbx_echo("Post-processing project directory")
    dev_cluster_name = "dev-%s-%s" % (kwargs["project_name"], LockFile.get("dbx_uuid"))

    dev_cluster_spec_path = os.path.join(SPECS_PATH, "config/dev/%s.json" % kwargs["cloud"].lower())
    shutil.copyfile(dev_cluster_spec_path, DEV_CLUSTER_FILE)

    update_json({"cluster_name": dev_cluster_name}, DEV_CLUSTER_FILE)

    shutil.copyfile(".gitignore.template", ".gitignore")
    subprocess.check_output('git init', shell=True)


def experiment_exists(experiment_path):
    return mlflow.get_experiment_by_name(experiment_path)


def verify_project_name(project_name):
    name_regex = r'^[_a-zA-Z][_a-zA-Z0-9]+$'
    if not re.match(name_regex, project_name):
        raise ValueError("Project name %s is not a valid python package name" % project_name)


def verify_job_name(job_name):
    name_regex = r'^[_a-zA-Z][_a-zA-Z0-9]+$'
    if not re.match(name_regex, job_name):
        raise ValueError("Job name %s is not a valid python package name" % job_name)
