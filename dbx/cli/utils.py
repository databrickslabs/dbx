import copy
import datetime as dt
import json
import os
import pathlib
import shutil
from typing import Dict, Any, Tuple

import click
import mlflow
import pkg_resources
from databricks_cli.configure.provider import ProfileConfigProvider
from databricks_cli.sdk.api_client import ApiClient
from path import Path
from retry import retry

from dbx import __version__

DBX_PATH = ".dbx"
INFO_FILE_PATH = "%s/project.json" % DBX_PATH
LOCK_FILE_PATH = "%s/lock.json" % DBX_PATH
DATABRICKS_MLFLOW_URI = "databricks"
DEPLOYMENT_TEMPLATE_PATH = pkg_resources.resource_filename('dbx', 'template/deployment.json')
DEFAULT_DEPLOYMENT_FILE_PATH = "%s/deployment.json" % DBX_PATH


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
        update_json({"context_id": context_id}, LOCK_FILE_PATH)

    @staticmethod
    def get_context() -> Any:
        return read_json(LOCK_FILE_PATH).get("context_id")


class InfoFile:

    @staticmethod
    def _create_dir() -> None:
        if not Path(DBX_PATH).exists():
            dbx_echo("dbx directory is not present, creating it")
            os.mkdir(DBX_PATH)

    @staticmethod
    def _create_deployment_file() -> None:
        if not Path(DEFAULT_DEPLOYMENT_FILE_PATH).exists():
            dbx_echo("dbx deployment file is not present, creating it from template")
            shutil.copy(DEPLOYMENT_TEMPLATE_PATH, DEFAULT_DEPLOYMENT_FILE_PATH)

    @staticmethod
    def _create_lock_file() -> None:
        if not Path(LOCK_FILE_PATH).exists():
            pathlib.Path(LOCK_FILE_PATH).write_text("{}")

    @staticmethod
    def initialize():

        InfoFile._create_dir()
        InfoFile._create_deployment_file()
        InfoFile._create_lock_file()

        init_content = {"environments": {}}
        write_json(init_content, INFO_FILE_PATH)

    @staticmethod
    def update(content: Dict[str, Any]) -> None:
        update_json(content, INFO_FILE_PATH)

    @staticmethod
    def get(item: str) -> Any:
        return read_json(INFO_FILE_PATH).get(item)


class ApiV12Client:
    def __init__(self, api_client: ApiClient):
        self.v1_client = copy.deepcopy(api_client)
        self.v1_client.url = self.v1_client.url.replace('/api/2.0', '/api/1.2')

    def get_command_status(self, payload) -> Dict[Any, Any]:
        result = self.v1_client.perform_query(method='GET', path='/commands/status', data=payload)
        return result

    def cancel_command(self, payload) -> None:
        self.v1_client.perform_query(method='POST', path='/commands/cancel', data=payload)

    def execute_command(self, payload) -> Dict[Any, Any]:
        result = self.v1_client.perform_query(method='POST',
                                              path='/commands/execute',
                                              data=payload)
        return result

    def get_context_status(self, payload):
        result = self.v1_client.perform_query(method='GET',
                                              path='/contexts/status', data=payload)
        return result

    def create_context(self, payload):
        result = self.v1_client.perform_query(method='POST',
                                              path='/contexts/create',
                                              data=payload)
        return result


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


def _get_api_client(profile: str) -> ApiClient:
    profile_config = ProfileConfigProvider(profile).get_config()
    api_client = ApiClient(host=profile_config.host, token=profile_config.token, command_name="dbx-%s" % __version__)
    return api_client


def _provide_environment(environment: str) -> Tuple[Dict[str, Any], ApiClient]:
    environment_data = InfoFile.get("environments").get(environment)

    if not environment_data:
        raise Exception("No environment %s provided in the deployment file" % environment)

    mlflow.set_tracking_uri("%s://%s" % (DATABRICKS_MLFLOW_URI, environment_data["profile"]))
    mlflow.set_experiment(environment_data["workspace_dir"])
    api_client = _get_api_client(environment_data["profile"])
    return environment_data, api_client


@retry(tries=10, delay=5, backoff=5)
def _upload_file(file_path: pathlib.Path):
    dbx_echo("Deploying file: %s" % file_path)
    mlflow.log_artifact(str(file_path), str(file_path.parent))
