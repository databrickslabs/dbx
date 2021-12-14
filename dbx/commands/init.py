from typing import List, Optional

import click
import pathlib
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS
import emoji
from dbx.utils.common import (
    dbx_echo,
)
from cookiecutter.main import cookiecutter
import pkg_resources

TEMPLATE_CHOICES = pkg_resources.resource_listdir("dbx", "templates")
TEMPLATE_ROOT_PATH = pathlib.Path(pkg_resources.resource_filename("dbx", "templates"))


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
    default="python_basic"
)
@click.option(
    "--template-parameters",
    multiple=True,
    type=str,
    help="""Additional parameters for project creation in the format of parameter=value, for example:

    .. code-block:

        dbx init --template-parameters project_name=some-name cloud=some_cloud

    """,
    default=None,
)
@debug_option
def init(template: str, template_parameters: Optional[List[str]]):
    dbx_echo(f"Configuring new project from template {template}")
    if not template_parameters:
        dbx_echo(
            "No template parameters were provided. "
            "Please follow the cookiecutter init dialogue to pass template parameters..."
        )
        template_parameters = {}
    else:
        template_parameters = {item.split("=")[0]: item.split("=")[1] for item in template_parameters}

    full_template_path = TEMPLATE_ROOT_PATH / template
    cookiecutter(str(full_template_path), extra_context=template_parameters)
    dbx_echo(emoji.emojize("Project configuration finished. You're all set to use dbx :fire:"))
