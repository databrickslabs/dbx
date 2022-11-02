import typer

from dbx.api.configure import ProjectConfigurationManager, EnvironmentInfo
from dbx.models.files.project import MlflowStorageProperties, StorageType
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
    enable_failsafe_cluster_reuse_with_assets: bool = typer.Option(
        False,
        "--enable-failsafe-cluster-reuse-with-assets",
        is_flag=True,
        help="""
        Enables failsafe behaviour for assets-based launches with definitions
        that are based on shared job clusters feature.

        This flag ignores any other flags.


        Project file should exist, otherwise command will fail.""",
    ),
    enable_context_based_upload_for_execute: bool = typer.Option(
        False,
        "--enable-context-based-upload-for-execute",
        is_flag=True,
        help="""
        Enables failsafe behaviour for assets-based launches with definitions
        that are based on shared job clusters feature.

        This flag ignores any other flags.


        Project file should exist, otherwise command will fail.""",
    ),
):
    manager = ProjectConfigurationManager()

    if enable_inplace_jinja_support:
        dbx_echo("Enabling jinja support")
        manager.enable_jinja_support()
        dbx_echo("✅ Enabling jinja support")

    elif enable_failsafe_cluster_reuse_with_assets:
        dbx_echo("Enabling failsafe cluster reuse with assets")
        manager.enable_failsafe_cluster_reuse()
        dbx_echo("✅ Enabling failsafe cluster reuse with assets")

    elif enable_context_based_upload_for_execute:
        dbx_echo("Enabling context-based upload for execute")
        manager.enable_context_based_upload_for_execute()
        dbx_echo("✅ Enabling context-based upload for execute")

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
