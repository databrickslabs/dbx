import pathlib
from copy import deepcopy
from typing import List

import click
import mlflow
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.cli.utils import parse_params, InfoFile, DATABRICKS_MLFLOW_URI, dbx_echo

adopted_context = deepcopy(CONTEXT_SETTINGS)

adopted_context.update(dict(
    ignore_unknown_options=True,
))


@click.command(context_settings=adopted_context,
               short_help="""
               Deploys project to artifact storage with given tags.
               Please provide either paths, or files or recursive globs to the FS objects which shall be deployed.
               """)
@click.option("--environment", required=True, type=str, help="Environment name")
@click.option("--dirs", required=False, type=str,
              help="Directories to be deployed, comma-separated")
@click.option("--files", required=False, type=str,
              help="Files to be deployed, comma-separated.")
@click.option("--rglobs", required=False, type=str,
              help="Recursive globs to select objects to be deployed, comma-separated.")
@click.argument('tags', nargs=-1, type=click.UNPROCESSED)
@debug_option
def deploy(environment: str, dirs: str, files: str, rglobs: str, tags: List[str]):
    deployment_tags = parse_params(tags)
    dbx_echo("Starting new deployment for environment %s" % environment)

    environment_data = InfoFile.get("environments").get(environment)

    mlflow.set_tracking_uri("%s://%s" % (DATABRICKS_MLFLOW_URI, environment_data["profile"]))
    mlflow.set_experiment(environment_data["workspace_dir"])

    prepared_fs_objects = []

    if files:
        prepared_fs_objects += preprocess_files(files)

    if dirs:
        prepared_fs_objects += preprocess_dirs(dirs)

    if rglobs:
        prepared_fs_objects += preprocess_rglobs(rglobs)

    distinct_file_paths = list(sorted(set(prepared_fs_objects)))

    if not distinct_file_paths:
        raise ValueError("No files found by given criteria. Please check command arguments and directory structure")

    with mlflow.start_run():
        for file_path in distinct_file_paths:
            dbx_echo("Deploying file %s" % file_path)
            mlflow.log_artifact(str(file_path), str(file_path.parent))

        deployment_tags.update({
            "dbx_action_type": "deploy",
            "dbx_environment": environment,
            "dbx_status": "SUCCESS"
        })

        mlflow.set_tags(deployment_tags)


def preprocess_files(files: str) -> List[pathlib.Path]:
    fs_objects = []
    for file_path in files.split(","):
        p_obj = pathlib.Path(file_path)
        if p_obj.is_file():
            fs_objects.append(p_obj)
        else:
            raise ValueError("Provided path to file %s is incorrect" % file_path)
    return fs_objects


def preprocess_dirs(dirs: str) -> List[pathlib.Path]:
    fs_objects = []
    for directory in dirs.split(","):
        p_obj = pathlib.Path(directory)
        if p_obj.is_dir():
            for selection_result in p_obj.rglob("*"):
                if selection_result.is_file():
                    fs_objects.append(selection_result)
        else:
            raise ValueError("Provided path to directory %s is incorrect" % directory)
    return fs_objects


def preprocess_rglobs(rglobs: str) -> List[pathlib.Path]:
    fs_objects = []
    for rglob in rglobs.split(","):
        dbx_echo("Checking files for rglob: %s" % rglob)
        rglob_selection = pathlib.Path(".").rglob(rglob)
        for path in rglob_selection:
            if path.is_file():
                fs_objects.append(path)
    return fs_objects
