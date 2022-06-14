import os
from pathlib import Path, PurePosixPath
from typing import Optional, Tuple

import git
import mlflow
import mlflow.entities
from databricks_cli.configure.config import _get_api_client  # noqa
from databricks_cli.configure.provider import (
    ProfileConfigProvider,
    EnvironmentVariableConfigProvider,
    DatabricksConfig,
)
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.workspace.api import WorkspaceService
from setuptools import sandbox

from dbx.api.configure import ProjectConfigurationManager
from dbx.constants import DATABRICKS_MLFLOW_URI
from dbx.utils import dbx_echo


def generate_filter_string(env: str) -> str:
    general_filters = [
        f"tags.dbx_environment = '{env}'",
        "tags.dbx_status = 'SUCCESS'",
        "tags.dbx_action_type = 'deploy'",
    ]

    branch_name = get_current_branch_name()
    if branch_name:
        general_filters.append(f"tags.dbx_branch_name = '{branch_name}'")

    all_filters = general_filters
    filter_string = " and ".join(all_filters)
    return filter_string


def _prepare_workspace_dir(api_client: ApiClient, ws_dir: str):
    p = str(PurePosixPath(ws_dir).parent)
    service = WorkspaceService(api_client)
    service.mkdirs(p)


def pick_config(profile: str) -> Tuple[str, DatabricksConfig]:
    config = EnvironmentVariableConfigProvider().get_config()
    if config:
        config_type = "ENV"
        dbx_echo("Using configuration from the environment variables")
    else:
        dbx_echo("No environment variables provided, using the ~/.databrickscfg")
        config = ProfileConfigProvider(profile).get_config()
        config_type = "PROFILE"
        if not config:
            raise Exception(f"""Couldn't get profile with name: {profile}. Please check the config settings""")
    return config_type, config


def prepare_environment(environment: str) -> ApiClient:
    environment_data = ProjectConfigurationManager().get(environment)

    config_type, config = pick_config(environment_data.profile)

    try:
        api_client = _get_api_client(config, command_name="cicdtemplates-")
    except IndexError as e:
        dbx_echo(
            """
        Error during initializing the API client.
        Probably, env variable DATABRICKS_HOST is set, but cannot be properly parsed.
        Please verify that DATABRICKS_HOST is in format https://<your-workspace-host>
        Original exception is below:
        """
        )
        raise e

    _prepare_workspace_dir(api_client, environment_data.workspace_dir)

    if config_type == "ENV":
        mlflow.set_tracking_uri(DATABRICKS_MLFLOW_URI)
    elif config_type == "PROFILE":
        mlflow.set_tracking_uri(f"{DATABRICKS_MLFLOW_URI}://{environment_data.profile}")
    else:
        raise NotImplementedError(f"Config type: {config_type} is not implemented")

    experiment: Optional[mlflow.entities.Experiment] = mlflow.get_experiment_by_name(environment_data.workspace_dir)

    # if there is no experiment
    if not experiment:
        mlflow.create_experiment(environment_data.workspace_dir, environment_data.artifact_location)
    else:
        # verify experiment location
        if experiment.artifact_location != environment_data.artifact_location:
            raise Exception(
                f"Required location of experiment {environment_data.workspace_dir} "
                f"doesn't match the project defined one: \n"
                f"\t experiment artifact location: {experiment.artifact_location} \n"
                f"\t project artifact location   : {environment_data.artifact_location} \n"
                f"Change of experiment location is currently not supported in MLflow. "
                f"Please change the experiment name to create a new experiment."
            )

    mlflow.set_experiment(environment_data.workspace_dir)

    return api_client


def get_package_file() -> Optional[Path]:
    dbx_echo("Locating package file")
    file_locator = list(Path("dist").glob("*.whl"))
    sorted_locator = sorted(file_locator, key=os.path.getmtime)  # get latest modified file, aka latest package version
    if sorted_locator:
        file_path = sorted_locator[-1]
        dbx_echo(f"Package file located in: {file_path}")
        return file_path
    else:
        dbx_echo("Package file was not found")
        return None


def handle_package(rebuild_arg):
    if rebuild_arg:
        dbx_echo("No rebuild will be done, please ensure that the package distribution is in dist folder")
    else:
        dbx_echo("Re-building package")
        if not Path("setup.py").exists():
            raise Exception(
                "No setup.py provided in project directory. Please create one, or disable rebuild via --no-rebuild"
            )
        sandbox.run_setup("setup.py", ["-q", "clean", "bdist_wheel"])
        dbx_echo("Package re-build finished")


def get_current_branch_name() -> Optional[str]:
    if "GITHUB_REF" in os.environ:
        ref = os.environ["GITHUB_REF"].split("/")
        return ref[-1]
    else:
        try:
            repo = git.Repo(".", search_parent_directories=True)
            if repo.head.is_detached:
                return None
            else:
                return repo.active_branch.name
        except git.InvalidGitRepositoryError:
            return None
