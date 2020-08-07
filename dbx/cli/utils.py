import datetime as dt
import json
import os
from typing import Union
from uuid import uuid4

import click

LOCK_FILE_NAME = ".dbx.lock.json"
INFO_FILE_NAME = ".dbx.json"


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


class LockFileController:

    def __init__(self):
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
    def get_uuid() -> Union[str, None]:
        return read_json(LOCK_FILE_NAME).get("dbx_uuid")

    @staticmethod
    def get_dev_cluster_id() -> Union[str, None]:
        return read_json(LOCK_FILE_NAME).get("dev_cluster_id")

    @staticmethod
    def get_execution_context_id() -> Union[str, None]:
        return read_json(LOCK_FILE_NAME).get("execution_context_id")


def provide_lockfile_controller(function):
    def decorator(*args, **kwargs):
        kwargs['lockfile_controller'] = LockFileController()
        return function(*args, **kwargs)

    return decorator


def dbx_echo(message):
    formatted_message = "[dbx][%s][%s]" % (dt.datetime.now(), message)
    click.echo(formatted_message)
