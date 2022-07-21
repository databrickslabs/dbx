import datetime as dt
from typing import Any

import click
import emoji


def dbx_echo(message: Any):
    formatted_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    formatted_message = f"[dbx][{formatted_time}] {message}"
    try:
        click.echo(emoji.emojize(formatted_message))
    # this is a fix for unicode error on some platforms as per https://github.com/databrickslabs/dbx/issues/121
    except UnicodeEncodeError:
        click.echo(formatted_message)
