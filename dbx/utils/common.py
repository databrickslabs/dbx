import os
from typing import Dict, List, Optional

import git
from databricks_cli.sdk.api_client import ApiClient

from dbx.api.auth import ProfileEnvConfigProvider
from dbx.api.client_provider import DatabricksClientProvider
from dbx.api.configure import ProjectConfigurationManager, EnvironmentInfo
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
    info = ProjectConfigurationManager().get(env_name)
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
    return ProjectConfigurationManager().get(environment)


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
