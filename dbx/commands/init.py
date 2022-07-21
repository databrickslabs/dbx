from typing import List, Optional

import click
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
    checkout: Optional[str],
    parameters: Optional[List[str]],
    no_input: bool = False,
):
    if template and path:
        raise Exception(
            "Both --template and --path options are not supported."
            "Please choose either built-in template or an external path"
        )
    if not path and template is None:
        template = DEFAULT_TEMPLATE

    msg_base = "Configuring new project from "
    msg = f"template {template} :gear:" if template else f"path {path} :link:"

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

    _path = path if path else str(TEMPLATE_ROOT_PATH / template / "render")
    cookiecutter(_path, extra_context=parameters, no_input=no_input, checkout=checkout)
    dbx_echo("Project configuration finished. You're all set to use dbx :fire:")
