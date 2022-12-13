import os
from functools import lru_cache
from typing import List, Optional

from databricks_cli.configure.provider import (
    DatabricksConfig,
    ProfileConfigProvider,
    DatabricksConfigProvider,
)

from dbx.utils import dbx_echo


class DbxConfig(DatabricksConfig):
    def __init__(
        self,
        host,
        username,
        password,
        token,
        refresh_token=None,
        insecure=None,
        jobs_api_version=None,
        azure_ad_token=None,
        workspace_id=None,
        org_id=None,
    ):  # noqa
        super().__init__(host, username, password, token, refresh_token, insecure, jobs_api_version)
        _headers = {
            "X-Databricks-Azure-SP-Management-Token": azure_ad_token,
            "X-Databricks-Azure-Workspace-Resource-Id": workspace_id,
            "X-Databricks-Org-Id": org_id,
        }
        self.headers = {k: v for k, v in _headers.items() if v is not None}


class DbxEnvironmentVariableConfigProvider(DatabricksConfigProvider):
    """Loads from system environment variables."""

    def get_config(self):
        host = os.environ.get("DATABRICKS_HOST")
        username = os.environ.get("DATABRICKS_USERNAME")
        password = os.environ.get("DATABRICKS_PASSWORD")
        token = os.environ.get("DATABRICKS_TOKEN")
        azure_ad_token = os.environ.get("AAD_TOKEN")
        refresh_token = os.environ.get("DATABRICKS_REFRESH_TOKEN")
        insecure = os.environ.get("DATABRICKS_INSECURE")
        jobs_api_version = os.environ.get("DATABRICKS_JOBS_API_VERSION")
        workspace_id = os.environ.get("WORKSPACE_ID")
        org_id = os.environ.get("ORG_ID")
        config = DbxConfig(
            host,
            username,
            password,
            token,
            refresh_token,
            insecure,
            jobs_api_version,
            azure_ad_token,
            workspace_id,
            org_id,
        )
        if config.is_valid:
            return config
        return None


class ProfileEnvConfigProvider(DatabricksConfigProvider):
    DBX_PROFILE_ENV = "DBX_CLI_PROFILE"

    def __init__(self):
        self.profile = self._get_profile_name()

    def get_config(self) -> Optional[DatabricksConfig]:
        profile = self._get_profile_name()
        _config = None if not profile else ProfileConfigProvider(profile).get_config()
        return _config

    @classmethod
    def _get_profile_name(cls) -> Optional[str]:
        return os.environ.get(cls.DBX_PROFILE_ENV)


class AuthConfigProvider:
    @staticmethod
    def _verify_config_validity(config: DbxConfig):
        if not config.is_valid_with_token:
            raise Exception(
                "Provided configuration is not based on token authentication."
                "Please switch to token-based authentication instead."
            )
        if not (config.host.startswith("https://") or config.host.startswith("http://")):
            raise Exception(
                "Provided host value doesn't start with https:// or http:// \n Please check the host configuration."
            )

    @staticmethod
    def _get_config_from_env() -> Optional[DbxConfig]:
        config = DbxEnvironmentVariableConfigProvider().get_config()
        if config:
            AuthConfigProvider._verify_config_validity(config)
            return config

    @classmethod
    @lru_cache(maxsize=None)
    def get_config(cls) -> DbxConfig:
        providers: List[DatabricksConfigProvider] = [
            DbxEnvironmentVariableConfigProvider(),
            ProfileEnvConfigProvider(),
        ]
        for provider in providers:
            config = provider.get_config()
            if config:
                dbx_echo(f"Found auth config from provider {provider.__class__.__name__}, verifying it")
                cls._verify_config_validity(config)
                dbx_echo(f"Found auth config from provider {provider.__class__.__name__}, verification successful")
                if hasattr(provider, "profile"):
                    dbx_echo(f"Profile {provider.profile} will be used for deployment")
                return config

        raise Exception(
            "No valid authentication information was provided.\n"
            "Please use one of the authentication methods:"
            "\t * Via environment variables DATABRICKS_HOST and DATABRICKS_TOKEN \n"
            f"\t * Via specifying the profile name in the env variable {ProfileEnvConfigProvider.DBX_PROFILE_ENV}"
            "\t * Via providing the profile in the .dbx/project.json file"
        )
