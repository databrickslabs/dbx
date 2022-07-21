import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

import git
from databricks_cli.sdk import ClusterService
from databricks_cli.sdk.api_client import ApiClient

from dbx.api.auth import ProfileEnvConfigProvider
from dbx.api.client_provider import DatabricksClientProvider
from dbx.api.configure import ConfigurationManager, EnvironmentInfo
from dbx.api.storage.mlflow_based import MlflowStorageConfigurationManager
from dbx.utils import dbx_echo


def parse_multiple(multiple_argument: List[str]) -> Dict[str, str]:
    tags_splitted = [t.split("=") for t in multiple_argument]
    tags_dict = {t[0]: t[1] for t in tags_splitted}
    return tags_dict


def transfer_profile_name(info: EnvironmentInfo):
    if not os.environ.get(ProfileEnvConfigProvider.DBX_PROFILE_ENV):
        dbx_echo("Using profile provided from the project file")
        os.environ[ProfileEnvConfigProvider.DBX_PROFILE_ENV] = info.profile
    else:
        dbx_echo(f"Using profile provided via the env variable {ProfileEnvConfigProvider.DBX_PROFILE_ENV}")


def prepare_environment(env_name: str) -> ApiClient:
    info = ConfigurationManager().get(env_name)
    transfer_profile_name(info)
    MlflowStorageConfigurationManager.prepare(info)
    return DatabricksClientProvider.get_v2_client()


def generate_filter_string(env: str, branch_name: Optional[str]) -> str:
    general_filters = [
        f"tags.dbx_environment = '{env}'",
        "tags.dbx_status = 'SUCCESS'",
        "tags.dbx_action_type = 'deploy'",
    ]

    if branch_name:
        general_filters.append(f"tags.dbx_branch_name = '{branch_name}'")

    all_filters = general_filters
    filter_string = " and ".join(all_filters)
    return filter_string


def get_environment_data(environment: str) -> EnvironmentInfo:
    environment_data = ConfigurationManager().get(environment)

    if not environment_data:
        raise Exception(f"No environment {environment} provided in the project file")
    return environment_data


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
            raise FileNotFoundError(
                "No setup.py provided in project directory. Please create one, or disable rebuild via --no-rebuild"
            )

        dist_path = Path("dist")
        if dist_path.exists():
            dbx_echo("dist folder already exists, cleaning it before build")
            for _file in dist_path.glob("*.whl"):
                _file.unlink()

        subprocess.check_call([sys.executable] + shlex.split("-m pip wheel -w dist -e . --prefer-binary --no-deps"))
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


def _preprocess_cluster_args(api_client: ApiClient, cluster_name: Optional[str], cluster_id: Optional[str]) -> str:
    cluster_service = ClusterService(api_client)

    if not cluster_name and not cluster_id:
        raise RuntimeError("Parameters --cluster-name and --cluster-id couldn't be empty at the same time.")

    if cluster_name:

        existing_clusters = [
            c for c in cluster_service.list_clusters().get("clusters") if not c.get("cluster_name").startswith("job-")
        ]
        matching_clusters = [c for c in existing_clusters if c.get("cluster_name") == cluster_name]

        if not matching_clusters:
            cluster_names = [c["cluster_name"] for c in existing_clusters]
            raise NameError(f"No clusters with name {cluster_name} found. Available clusters are: {cluster_names} ")
        if len(matching_clusters) > 1:
            raise NameError(f"Found more then one cluster with name {cluster_name}: {matching_clusters}")

        cluster_id = matching_clusters[0]["cluster_id"]
    else:
        if not cluster_service.get_cluster(cluster_id):
            raise NameError(f"Cluster with id {cluster_id} not found")

    return cluster_id
