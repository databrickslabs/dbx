import copy
import datetime as dt
import json
import os
import pathlib
import shutil
from typing import Dict, Any, List

import click
import mlflow
import pkg_resources
import requests
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import ProfileConfigProvider, DEFAULT_SECTION
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.workspace.api import WorkspaceService
from path import Path
from retry import retry
import logging
from typing import NamedTuple
import paramiko
from paramiko.client import SSHClient
from databricks_cli.dbfs.api import DbfsService

DBX_PATH = ".dbx"
INFO_FILE_PATH = "%s/project.json" % DBX_PATH
LOCK_FILE_PATH = "%s/lock.json" % DBX_PATH
DATABRICKS_MLFLOW_URI = "databricks"
DEPLOYMENT_TEMPLATE_PATH = pkg_resources.resource_filename('dbx', 'template/deployment.json')
CONF_PATH = "conf"
DEFAULT_DEPLOYMENT_FILE_PATH = "%s/deployment.json" % CONF_PATH

FORMAT = u'[%(asctime)s] %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)
logging.getLogger("paramiko").setLevel(logging.ERROR)


class TunnelInfo(NamedTuple):
    host: str
    port: int
    private_key_file: str


logger = logging.getLogger(__name__)


def parse_multiple(multiple_argument: List[str]) -> Dict[str, str]:
    tags_splitted = [t.split("=") for t in multiple_argument]
    tags_dict = {t[0]: t[1] for t in tags_splitted}
    return tags_dict


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

    @staticmethod
    def set_url(ssh_url: str) -> None:
        update_json({"ssh_url": ssh_url}, LOCK_FILE_PATH)

    @staticmethod
    def get_url() -> Any:
        return read_json(LOCK_FILE_PATH).get("ssh_url")


class DeploymentFile:
    def __init__(self, path):
        self._path = path

    def get_environment(self, environment: str) -> Any:
        return read_json(self._path).get(environment)

    def update_environment(self, environment: str, content):
        environment_data = self.get_environment(environment)
        environment_data.update(content)
        update_json({environment: environment_data}, self._path)


class InfoFile:

    @staticmethod
    def _create_dir() -> None:
        if not Path(DBX_PATH).exists():
            dbx_echo("dbx directory is not present, creating it")
            os.mkdir(DBX_PATH)

    @staticmethod
    def _create_deployment_file() -> None:
        if not Path(CONF_PATH).exists():
            os.mkdir(CONF_PATH)
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
        if not pathlib.Path(INFO_FILE_PATH).exists():
            raise Exception("Your project is not yet configured, please configure it via `dbx configure`")
        return read_json(INFO_FILE_PATH).get(item)


class ApiV1Client:
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
        try:
            result = self.v1_client.perform_query(method='GET',
                                                  path='/contexts/status', data=payload)
            return result
        except requests.exceptions.HTTPError:
            return None

    # sometimes cluster is already in the status="RUNNING", however it couldn't yet provide execution context
    # to make the execute command stable is such situations, we add retry handler.
    @retry(tries=10, delay=5, backoff=5)
    def create_context(self, payload):
        result = self.v1_client.perform_query(method='POST',
                                              path='/contexts/create',
                                              data=payload)
        return result


def dbx_echo(message: str):
    formatted_message = "[dbx][%s] %s" % (dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], message)
    click.echo(formatted_message)


def generate_filter_string(env: str, tags: Dict[str, str]) -> str:
    env_filter = ['tags.dbx_environment="%s"' % env]
    # we are not using attribute.status due to it's behaviour with nested runs
    status_filter = ['tags.dbx_status="SUCCESS"']
    deploy_filter = ['tags.dbx_action_type="deploy"']
    tags_filter = ['tags.%s="%s"' % (k, v) for k, v in tags.items()]
    filters = status_filter + deploy_filter + env_filter + tags_filter
    filter_string = " and ".join(filters)
    return filter_string


def environment_option(f):
    return click.option('-e', '--environment', required=True, default=None,
                        help='Environment name.')(f)


def profile_option(f):
    return click.option('--profile', required=False, default=DEFAULT_SECTION,
                        help='CLI connection profile to use. The default profile is "DEFAULT".')(f)


def _prepare_workspace_dir(api_client: ApiClient, ws_dir: str):
    p = str(pathlib.Path(ws_dir).parent)
    service = WorkspaceService(api_client)
    service.mkdirs(p)


def prepare_environment(environment: str):
    environment_data = InfoFile.get("environments").get(environment)

    if not environment_data:
        raise Exception("No environment %s provided in the project file" % environment)

    mlflow.set_tracking_uri("%s://%s" % (DATABRICKS_MLFLOW_URI, environment_data["profile"]))
    config = ProfileConfigProvider(environment_data["profile"]).get_config()
    api_client = _get_api_client(config, command_name="cicdtemplates-")
    _prepare_workspace_dir(api_client, environment_data["workspace_dir"])

    experiment = mlflow.get_experiment_by_name(environment_data["workspace_dir"])

    if not experiment:
        mlflow.create_experiment(environment_data["workspace_dir"], environment_data["artifact_location"])

    mlflow.set_experiment(environment_data["workspace_dir"])

    return api_client


class FileUploader:
    def __init__(self, api_client: ApiClient):
        self._dbfs_service = DbfsService(api_client)

    def file_exists(self, file_path: str):
        try:
            self._dbfs_service.get_status(file_path)
            return True
        except:
            return False

    @retry(tries=10, delay=5, backoff=5)
    def upload_file(self, file_path: pathlib.Path):
        dbx_echo("Deploying file: %s" % file_path)
        mlflow.log_artifact(str(file_path), str(file_path.parent))


def get_ssh_client(info: TunnelInfo) -> SSHClient:
    client = SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=info.host,
        port=info.port,
        username='root',
        key_filename=info.private_key_file
    )
    transport = client.get_transport()
    transport.set_keepalive(30)
    return client
