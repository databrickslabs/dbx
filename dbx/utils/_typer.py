from typing import Callable

from typer import Typer


def add_callback(app: Typer, func: Callable):
    app.callback()(func)
