import datetime as dt
from pathlib import Path
from typing import Any
from rich import print


def dbx_echo(message: Any):
    formatted_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    formatted_message = f"[red]\[dbx][/red][:hourglass_flowing_sand:{formatted_time}] {message}"
    print(formatted_message)


def current_folder_name() -> str:
    return Path(".").absolute().name
