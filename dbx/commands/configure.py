from typing import Optional

import click
from databricks_cli.configure.config import debug_option
from databricks_cli.configure.provider import DEFAULT_SECTION
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.api.configure import ConfigurationManager, EnvironmentInfo
from dbx.utils import dbx_echo
from dbx.utils.options import environment_option, profile_option


@click.command(
    context_settings=CONTEXT_SETTINGS,
    short_help="Configures project environment in the current folder.",
    help="""
    Configures project environment in the current folder.

    This command might be used multiple times to change configuration of a given environment.
    If project file (located in :code:`.dbx/project.json`) is non-existent, it will be initialized.
    There is no strict requirement to configure project file via this command.
    You can also configure it directly via any file editor.
    """,
)
@click.option(
    "--workspace-dir",
    required=False,
    type=str,
    help="""Workspace directory for MLflow experiment. \n
         If not provided, default directory will be :code:`/Shared/dbx/projects/<current-folder-name>`.""",
    default=None,
)
@click.option(
    "--artifact-location",
    required=False,
    type=str,
    help="""Artifact location in DBFS. \n
        If not provided, default location will be :code:`dbfs:/dbx/<current-folder-name>`.""",
    default=None,
)
@environment_option
@debug_option
@profile_option
def configure(
    environment: str,
    workspace_dir: Optional[str] = None,
    artifact_location: Optional[str] = None,
    profile: Optional[str] = DEFAULT_SECTION,
):
    dbx_echo(f"Configuring new environment with name {environment}")
    manager = ConfigurationManager()
    manager.create_or_update(environment, EnvironmentInfo(profile, workspace_dir, artifact_location))
    dbx_echo("Environment configuration successfully finished")
