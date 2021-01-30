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
    context_settings=CONTEXT_SETTINGS, short_help="Configures new environment."
)
@click.option(
    "--workspace-dir",
    required=False,
    type=str,
    help="Workspace directory for MLflow experiment.",
    default=None,
)
@click.option(
    "--artifact-location",
    required=False,
    type=str,
    help="Artifact location (dbfs path)",
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
        workspace_dir = "/Shared/dbx/projects/%s" % Path(".").absolute().name
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
