from pathlib import Path

import click
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.utils.common import (
    InfoFile,
    dbx_echo,
    INFO_FILE_PATH,
    environment_option,
    profile_option,
)


@click.command(
    context_settings=CONTEXT_SETTINGS,
    short_help="Configures project environment in the current folder.",
    help="""
    Configures project environment in the current folder.
    
    This command might be used multiple times to change configuration of a given environment.
    If project file (located in :code:`.dbx/project.json`) is non-existent, it will be initialized.
    There is no strict requirement to configure project file via this command.
    You can also configure it directly via any file editor.
    """
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
        environment: str, workspace_dir: str, artifact_location: str, profile: str
):
    dbx_echo("Configuring new environment with name %s" % environment)

    if not workspace_dir:
        workspace_dir = f'/Shared/dbx/projects/{Path(".").absolute().name}'
        dbx_echo(
            f"Workspace directory argument is not provided, using the following directory: {workspace_dir}"
        )

    if not Path(INFO_FILE_PATH).exists():
        InfoFile.initialize()

    if InfoFile.get("environments").get(environment):
        dbx_echo(f"Environment {environment} will be overridden with new properties")

    if not artifact_location:
        artifact_location = f'dbfs:/dbx/{Path(".").absolute().name}'

    environment_info = {
        environment: {
            "profile": profile,
            "workspace_dir": workspace_dir,
            "artifact_location": artifact_location,
        }
    }

    environments = InfoFile.get("environments")

    environments.update(environment_info)

    InfoFile.update({"environments": environments})
    dbx_echo("Environment configuration successfully finished")
