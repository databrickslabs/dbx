import base64
import datetime as dt
import glob
import json
import os
import pathlib
import warnings
from typing import Dict
from uuid import uuid4

import click
import mlflow
from databricks_cli.click_types import ContextObject
from databricks_cli.configure.config import get_profile_from_context
from databricks_cli.dbfs.api import DbfsService
from setuptools import sandbox

# disables warnings coming from wheel_inspect dependency
warnings.filterwarnings("ignore", category=DeprecationWarning)

import wheel_inspect

LOCK_FILE_NAME = ".dbx.lock.json"
INFO_FILE_NAME = ".dbx.json"
DATABRICKS_MLFLOW_URI = "databricks"


def read_json(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


def write_json(content, file_path):
    with open(file_path, "w") as f:
        json.dump(content, f, indent=4)


def update_json(new_content, file_path):
    content = read_json(file_path)
    content.update(new_content)
    write_json(content, file_path)


class InfoFile:

    @staticmethod
    def initialize(content):
        write_json(content, INFO_FILE_NAME)

    @staticmethod
    def update(content) -> None:
        update_json(content, INFO_FILE_NAME)

    @staticmethod
    def get(item):
        return read_json(INFO_FILE_NAME).get(item)


class LockFile:

    @staticmethod
    def initialize():
        dbx_uuid = str(uuid4())
        payload = {"dbx_uuid": dbx_uuid}
        if not os.path.exists(LOCK_FILE_NAME):
            write_json(payload, LOCK_FILE_NAME)
        else:
            content = read_json(LOCK_FILE_NAME)
            if not content.get("dbx_uuid"):
                write_json(payload, LOCK_FILE_NAME)

    @staticmethod
    def update(content) -> None:
        update_json(content, LOCK_FILE_NAME)

    @staticmethod
    def get(item):
        return read_json(LOCK_FILE_NAME).get(item)


def provide_lockfile_controller(function):
    def decorator(*args, **kwargs):
        kwargs['lockfile'] = LockFile
        return function(*args, **kwargs)

    return decorator


def dbx_echo(message):
    formatted_message = "[dbx][%s] %s" % (dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], message)
    click.echo(formatted_message)


def setup_mlflow(function):
    def decorator(*args, **kwargs):
        mlflow.set_tracking_uri("%s://%s" % (DATABRICKS_MLFLOW_URI, get_profile_from_context()))
        mlflow.set_experiment(InfoFile.get("experiment_path"))
        return function(*args, **kwargs)

    return decorator


# allows to pick profile value directly from info file, but with fallback if option is provided
def custom_profile_option(f):
    def callback(ctx, _, value):
        context_object = ctx.ensure_object(ContextObject)
        if value is not None:
            context_object.set_profile(value)
        else:
            profile = InfoFile.get("profile")
            if profile:
                context_object.set_profile(profile)

    return click.option('--profile', required=False, default=None, callback=callback,
                        expose_value=False,
                        help='CLI connection profile to use. The default profile is "DEFAULT".')(f)


def extract_version(whl_file) -> str:
    inspection_data = wheel_inspect.inspect_wheel(whl_file)
    return inspection_data["version"]


def build_project_whl() -> str:
    sandbox.run_setup('setup.py', ['-q', 'clean', 'bdist_wheel'])
    whl_file = glob.glob("dist/*.whl")[0]
    return whl_file


def upload_file(file_path):
    dbx_echo("Uploading file to artifactory: %s" % file_path)
    prefix, path = os.path.split(file_path)
    mlflow.log_artifact(file_path, artifact_path=prefix)


def parse_tags(tags):
    contains_equals = sum(["=" in t for t in tags])
    if contains_equals == len(tags):
        # if the format is --tag1=value2 --tag2=value2
        formatted = {t.split("=")[0].replace("--", "").replace("-", "_"): t.split("=")[-1] for t in tags}
    else:
        # format is --tag1 value1 --tag2 value2
        if len(tags) % 2 != 0:
            raise NameError("Given tags are not in compatible format (either --tag1=value1 or --tag1 value1).")
        else:
            keys = tags[::2]
            values = tags[1::2]
            formatted = {keys[idx].replace("--", "").replace("-", "_"): values[idx] for idx in range(len(keys))}

    return formatted


def generate_filter_string(env, tags: Dict[str, str]):
    tags_filter = ['tags.%s="%s"' % (key, value) for key, value in tags.items()]
    env_filter = ['tags.dbx_env="%s"' % env]

    # we are not using attribute.status due to it's behaviour with nested runs
    status_filter = ['tags.dbx_status="SUCCESS"']
    deploy_filter = ['tags.action_type="deploy"']

    filters = tags_filter + status_filter + deploy_filter + env_filter
    filter_string = " and ".join(filters)
    return filter_string


def upload_configs():
    configs = list(pathlib.Path('config').rglob('job.json'))
    for conf in configs:
        upload_file(conf)


def upload_entrypoints(project_name):
    entrypoints = list(pathlib.Path(project_name).rglob('entrypoint.py'))
    for ep in entrypoints:
        upload_file(ep)


def get_dist(api_client, artifact_uri):
    dist_uri = "%s/dist" % artifact_uri
    dbfs_service = DbfsService(api_client)
    whl_file = dbfs_service.list(dist_uri)["files"][0]["path"]
    return whl_file


def prepare_job_config(api_client, package_path, config_path, entrypoint_path):
    dbfs_service = DbfsService(api_client)
    raw_config_payload = dbfs_service.read(config_path)["data"]
    config_payload = base64.b64decode(raw_config_payload).decode("utf-8")
    config = json.loads(config_payload)
    package_info = [
        {"whl": package_path}
    ]
    config["libraries"] = package_info
    config["spark_python_task"] = {
        "python_file": entrypoint_path
    }

    return config
