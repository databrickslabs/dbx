import typer

from dbx.api.configure import ProjectConfigurationManager, EnvironmentInfo
from dbx.models.project import MlflowStorageProperties, StorageType
from dbx.options import ENVIRONMENT_OPTION, PROFILE_OPTION
from dbx.utils import dbx_echo, current_folder_name


def configure(
    environment: str = ENVIRONMENT_OPTION,
    workspace_dir: str = typer.Option(
        f"/Shared/dbx/projects/{current_folder_name()}",
        "--workspace-dir",
        "--workspace-directory",
        help="Workspace directory for MLflow experiment.",
        show_default=False,
    ),
    artifact_location: str = typer.Option(
        f"dbfs:/dbx/{current_folder_name()}",
        "--artifact-location",
        help="Artifact location in DBFS or in the cloud storage",
        show_default=False,
    ),
    profile: str = PROFILE_OPTION,
    enable_inplace_jinja_support: bool = typer.Option(
        False,
        "--enable-inplace-jinja-support",
        is_flag=True,
        help="""Enables inplace jinja support for deployment files.


        This flag ignores any other flags.


        Project file should exist, otherwise command will fail.""",
    ),
):
    manager = ProjectConfigurationManager()

    if enable_inplace_jinja_support:
        dbx_echo("Enabling jinja support")
        manager.enable_jinja_support()
        dbx_echo("✅ Enabling jinja support")

    else:
        dbx_echo(f"Configuring new environment with name {environment}")
        manager.create_or_update(
            environment,
            EnvironmentInfo(
                profile=profile,
                storage_type=StorageType.mlflow,
                properties=MlflowStorageProperties(
                    workspace_directory=workspace_dir, artifact_location=artifact_location
                ),
            ),
        )
        dbx_echo("Environment configuration successfully finished")
