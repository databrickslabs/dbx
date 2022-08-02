from typing import List, Optional
from pathlib import Path

import click
import pkg_resources
from cookiecutter.main import cookiecutter
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.utils import dbx_echo
from dbx.constants import TEMPLATE_CHOICES, TEMPLATE_ROOT_PATH

DEFAULT_TEMPLATE = "python_basic"


@click.command(
    context_settings=CONTEXT_SETTINGS,
    short_help="Generates new project from the template",
    help="""
    Generates new project from the template

    Launching this command without :code:`--template-parameters` argument
    will open cookiecutter dialogue to enter the required parameters.
    """,
)
@click.option(
    "--template",
    required=False,
    type=click.Choice(TEMPLATE_CHOICES, case_sensitive=True),
    help="""Built-in dbx template used to kickoff the project.""",
    default=None,
)
@click.option(
    "--path",
    required=False,
    help="""External template used to kickoff the project. Cannot be used together with :code:`--template` option.""",
)
@click.option(
    "--package",
    required=False,
    help="""Python package containing external template used to kickoff the project.
    Cannot be used together with :code:`--template` option.""",
)
@click.option(
    "--checkout",
    required=False,
    help="""Checkout argument for cookiecutter. Used only if :code:`--path` is used.""",
    default=None,
)
@click.option(
    "--parameters",
    "-p",
    multiple=True,
    type=str,
    help="""Additional parameters for project creation in the format of parameter=value, for example:

    .. code-block:

        dbx init --p project_name=some-name -p cloud=some_cloud

    """,
    default=None,
)
@click.option("--no-input", type=bool, required=False, is_flag=True)
@debug_option
def init(
    template: Optional[str],
    path: Optional[str],
    package: Optional[str],
    checkout: Optional[str],
    parameters: Optional[List[str]],
    no_input: bool = False,
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
