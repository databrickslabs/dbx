import datetime as dt
import json
import os
from uuid import uuid4

import click
import mlflow
import wheel_inspect
from databricks_cli.click_types import ContextObject
from databricks_cli.configure.config import get_profile_from_context
from setuptools import sandbox

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
    whl_file = os.listdir("dist")[0]
    full_whl_file_name = "dist/%s" % whl_file
    return full_whl_file_name


def upload_whl(full_whl_file_name):
    dbx_echo("Uploading package to DBFS")
    mlflow.log_artifact(full_whl_file_name, artifact_path="dist")


def parse_tags(tags):
    formatted = {t.split("=")[0].replace("--", "").replace("-", "_"): t.split("=")[-1] for t in tags}
    return formatted
