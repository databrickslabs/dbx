import typer

from dbx.api.configure import ConfigurationManager, EnvironmentInfo
from dbx.utils import dbx_echo, current_folder_name
from dbx.options import ENVIRONMENT_OPTION, PROFILE_OPTION


def configure(
    environment: str = ENVIRONMENT_OPTION,
    workspace_dir: str = typer.Option(
        f"/Shared/dbx/projects/{current_folder_name()}",
        "--workspace-dir",
        help="Workspace directory for MLflow experiment.",
    ),
    artifact_location: str = typer.Option(
        f"dbfs:/dbx/{current_folder_name()}", "--artifact-location", help="Artifact location in DBFS"
    ),
    profile: str = PROFILE_OPTION,
):
    dbx_echo(f"Configuring new environment with name {environment}")
    manager = ConfigurationManager()
    manager.create_or_update(environment, EnvironmentInfo(profile, workspace_dir, artifact_location))
    dbx_echo("Environment configuration successfully finished")
