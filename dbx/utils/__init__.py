import datetime as dt
from pathlib import Path
from typing import Any

import typer
from rich import print as rich_print
from typer.rich_utils import _get_rich_console # noqa

CONSOLE = _get_rich_console()


def format_dbx_message(message: Any) -> str:
    formatted_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    formatted_message = f"[red]\[dbx][/red][{formatted_time}] {message}"  # noqa
    return formatted_message


def dbx_echo(message: Any):
    formatted_message = format_dbx_message(message)
    try:
        CONSOLE.print(formatted_message, soft_wrap=True)
    except (UnicodeDecodeError, UnicodeEncodeError, UnicodeError):
        # fallback to the standard print behaviour
        formatted_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        formatted_message = f"[dbx][{formatted_time}] {message}"
        typer.echo(formatted_message)


def current_folder_name() -> str:
    return Path(".").absolute().name
