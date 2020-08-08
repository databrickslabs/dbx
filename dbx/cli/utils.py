import datetime as dt
import json
import os
from uuid import uuid4

import click
import mlflow
from databricks_cli.configure.config import get_profile_from_context

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
