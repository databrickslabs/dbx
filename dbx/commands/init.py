from typing import List, Optional

import click
from cookiecutter.main import cookiecutter
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.utils.common import dbx_echo, TEMPLATE_CHOICES, TEMPLATE_ROOT_PATH


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
    help="""Template used to kickoff the project.""",
    default="python_basic",
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
def init(template: str, parameters: Optional[List[str]], no_input: bool = False):
    dbx_echo(f"Configuring new project from template {template} :gear:")
    if not parameters:
        dbx_echo(
            "No template parameters were provided. "
            "Please follow the cookiecutter init dialogue to pass template parameters..."
        )
        parameters = {}
    else:
        splits = [item.split("=") for item in parameters]
        parameters = {item[0]: item[1] for item in splits}

    renderable_template_path = TEMPLATE_ROOT_PATH / template / "render"
    cookiecutter(str(renderable_template_path), extra_context=parameters, no_input=no_input)
    dbx_echo("Project configuration finished. You're all set to use dbx :fire:")
