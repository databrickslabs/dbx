from typing import List, Optional

import click
import emoji
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.utils.common import (
    dbx_echo,
)


@click.command(
    context_settings=CONTEXT_SETTINGS,
    short_help="Creates new project in the current folder",
    help="""
    Creates new project in the current folder.

    Launching this command without --template-parameters argument will open cookiecutter dialogue to enter the required parameters.
    """,
)
@click.option(
    "--template", required=False, type=str, help="""Template used to kickoff the project.""", default="python_basic"
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
    dbx_echo(emoji.emojize("Project configuration finished. You're all set to use dbx :fire:"))
