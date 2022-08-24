from pathlib import Path
from typing import List, Optional

import pkg_resources
import typer
from cookiecutter.main import cookiecutter

from dbx.constants import TEMPLATE_ROOT_PATH
from dbx.utils import dbx_echo

DEFAULT_TEMPLATE = "python_basic"


def init(
    template: Optional[str] = typer.Option(
        None, "--template", help="Built-in dbx template used to kickoff the project."
    ),
    path: Optional[str] = typer.Option(
        None,
        "--path",
        help="""External template used to kickoff the project.

            Cannot be used together with `--template` option.""",
    ),
    package: Optional[str] = typer.Option(
        None,
        "--package",
        help="""Python package containing external template used to kickoff the project.

            Cannot be used together with --template option.""",
    ),
    checkout: Optional[str] = typer.Option(
        None,
        "--checkout",
        help="""Checkout argument for cookiecutter. Used only if `--path` is used.""",
    ),
    parameters: Optional[List[str]] = typer.Option(
        None,
        "--parameters",
        "-p",
        help="""Additional parameters for project creation.

            Example: `dbx init -p cloud=some_cloud`
            """,
    ),
    no_input: bool = typer.Option(False, "--no-input", is_flag=True, help="Disables interactive input."),
):
    if sum([bool(template), bool(package), bool(path)]) > 1:
        raise Exception(
            "Only one option among  --template, --path and --package is supported."
            "Please choose either built-in template or python package containing dbx template "
            "or external path to cookiecutter template!"
        )
    if not path and not package and template is None:
        template = DEFAULT_TEMPLATE

    msg_base = "Configuring new project from "
    msg = f"template {template} :gear:" if template else f"path {path} :link:" if path else f"package {package} :link:"

    dbx_echo(msg_base + msg)

    if not parameters:
        dbx_echo(
            "No template parameters were provided. "
            "Please follow the cookiecutter init dialogue to pass template parameters..."
        )
        parameters = {}
    else:
        splits = [item.split("=") for item in parameters]
        parameters = {item[0]: item[1] for item in splits}

    if path:
        _path = path
    elif package:
        _path = str(Path(pkg_resources.resource_filename(package, "render")))
    else:
        _path = str(TEMPLATE_ROOT_PATH / template / "render")

    cookiecutter(_path, extra_context=parameters, no_input=no_input, checkout=checkout)
    dbx_echo("Project configuration finished. You're all set to use dbx :fire:")
