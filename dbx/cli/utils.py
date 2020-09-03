import datetime as dt
import json
import os
import pathlib
import shutil
from copy import deepcopy
from typing import Dict, Any, Tuple

import click
import mlflow
from databricks_cli.configure.provider import ProfileConfigProvider
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.utils import CONTEXT_SETTINGS
from retry import retry

import dbx

INFO_FILE_NAME = ".dbx/project.json"
LOCK_FILE_NAME = ".dbx/lock.json"
DATABRICKS_MLFLOW_URI = "databricks"
DEPLOYMENT_TEMPLATE_PATH = os.path.join(dbx.__path__[0], "template", "deployment.jsonnet")


def read_json(file_path: str) -> Dict[str, Any]:
    with open(file_path, "r") as f:
        return json.load(f)


def write_json(content: Dict[str, Any], file_path: str):
    with open(file_path, "w") as f:
        json.dump(content, f, indent=4)


def update_json(new_content: Dict[str, Any], file_path: str):
    content = read_json(file_path)
    content.update(new_content)
    write_json(content, file_path)


class ContextLockFile:

    @staticmethod
    def set_context(context_id: str) -> None:
        update_json({"context_id": context_id}, LOCK_FILE_NAME)

    @staticmethod
    def get_context() -> Any:
        return read_json(LOCK_FILE_NAME).get("context_id")


class InfoFile:

    @staticmethod
    def initialize(content: Dict[str, Any]):

        if not os.path.exists(".dbx"):
            os.mkdir(".dbx")

        if not os.path.exists(".dbx/deployment.jsonnet"):
            shutil.copy(DEPLOYMENT_TEMPLATE_PATH, ".dbx/deployment.jsonnet")

        if not os.path.exists(LOCK_FILE_NAME):
            pathlib.Path(LOCK_FILE_NAME).write_text("{}")
        write_json(content, INFO_FILE_NAME)

    @staticmethod
    def update(content: Dict[str, Any]) -> None:
        update_json(content, INFO_FILE_NAME)

    @staticmethod
    def get(item: str) -> Any:
        return read_json(INFO_FILE_NAME).get(item)


def dbx_echo(message: str):
    formatted_message = "[dbx][%s] %s" % (dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], message)
    click.echo(formatted_message)


def _generate_filter_string(env: str):
    env_filter = ['tags.dbx_environment="%s"' % env]

    # we are not using attribute.status due to it's behaviour with nested runs
    status_filter = ['tags.dbx_status="SUCCESS"']
    deploy_filter = ['tags.dbx_action_type="deploy"']

    filters = status_filter + deploy_filter + env_filter
    filter_string = " and ".join(filters)
    return filter_string


def _provide_environment(environment: str) -> Tuple[Dict[str, Any], ApiClient]:
    environment_data = InfoFile.get("environments").get(environment)
    mlflow.set_tracking_uri("%s://%s" % (DATABRICKS_MLFLOW_URI, environment_data["profile"]))
    mlflow.set_experiment(environment_data["workspace_dir"])
    profile_config = ProfileConfigProvider(environment_data["profile"]).get_config()
    api_client = ApiClient(host=profile_config.host, token=profile_config.token)
    return environment_data, api_client


def _adjust_context():
    new_context = deepcopy(CONTEXT_SETTINGS)
    new_context.update(dict(
        ignore_unknown_options=True,
    ))
    return new_context


@retry(tries=10, delay=5, backoff=5)
def _upload_file(file_path: pathlib.Path):
    dbx_echo("Deploying file: %s" % file_path)
    mlflow.log_artifact(str(file_path), str(file_path.parent))
