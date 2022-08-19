import datetime as dt
from pathlib import Path
from typing import Any

import typer
from rich import print as rich_print


def dbx_echo(message: Any):
    formatted_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    formatted_message = f"[red]\[dbx][/red][{formatted_time}] {message}"  # noqa
    try:
        rich_print(formatted_message)
    except (UnicodeDecodeError, UnicodeEncodeError, UnicodeError):
        # fallback to the standard print behaviour
        formatted_message = f"[dbx][{formatted_time}] {message}"
        typer.echo(formatted_message)


def current_folder_name() -> str:
    return Path(".").absolute().name
