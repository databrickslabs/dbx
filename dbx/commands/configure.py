from pathlib import Path
from typing import Optional, List

import click
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.api.configure import ProjectConfigurationManager
from dbx.models.project import MlflowArtifactStorageInfo, MlflowArtifactStorageProperties
from dbx.utils import dbx_echo
from dbx.utils.options import environment_option


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
    "--storage-type",
    required=True,
    type=str,
    help="""
        Storage type for the environment.

        If not provided, default storage type will be Mlflow experiment with following properties:

        * workspace_directory: directory where the Mlflow experiment will be located
        * artifact_location: artifact location for Mlflow experiment.

        Currently only Mlflow-based experiments are supported as artifact location.
        """,
    default="mlflow",
)
@click.option(
    "--properties",
    "-p",
    multiple=True,
    type=str,
    help="""
    Properties for the chosen storage type.
    """,
    default=None,
)
@environment_option
@debug_option
def configure(
    environment: str,
    storage_type: str,
    properties: Optional[List[str]],
):
    dbx_echo(f"Configuring new environment with name {environment}")
    manager = ProjectConfigurationManager()
    _properties = get_default_properties()

    if properties:
        provided_properties = dict(tuple(item.split("=")) for item in properties)  # noqa
        _properties.update(provided_properties)

    manager.create_or_update(
        environment, MlflowArtifactStorageInfo(properties=MlflowArtifactStorageProperties(**_properties))
    )
    dbx_echo("Environment configuration successfully finished")


def get_default_properties():
    current_path_name = Path(".").absolute().name
    props = {
        "workspace_directory": f"/Shared/dbx/projects/{current_path_name}",
        "artifact_location": f"dbfs:/dbx/{current_path_name}",
    }
    return props
