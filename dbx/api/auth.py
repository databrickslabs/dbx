import os
from typing import Optional

from databricks_cli.configure.provider import DatabricksConfig, ProfileConfigProvider, EnvironmentVariableConfigProvider

from dbx.utils import dbx_echo


class AuthConfigProvider:
    DBX_PROFILE_ENV = "DBX_CLI_PROFILE"

    @staticmethod
    def _get_config_from_profile(profile_env_variable: str) -> Optional[DatabricksConfig]:
        profile = os.environ.get(profile_env_variable)
        if not profile:
            dbx_echo(
                f"Environment variable {profile_env_variable} is not provided."
                f"Looking for host and token environment variables..."
            )
            return None
        else:
            dbx_echo(f"Environment variable {profile_env_variable} is provided, verifying the profile")
            config = ProfileConfigProvider(profile).get_config()
            if not config.is_valid_with_token():
                raise Exception(
                    f"Provided profile {profile} is not using token-based authentication."
                    f"Please switch to token-based authentication instead."
                )
            return config

    @staticmethod
    def _get_config_from_env() -> Optional[DatabricksConfig]:
        config = EnvironmentVariableConfigProvider().get_config()
        if config:
            if not config.is_valid_with_token():
                raise Exception(
                    "Provided environment variable configuration is not based on token authentication."
                    "Please switch to token-based authentication instead."
                )
            else:
                return config

    def __init__(self):
        self.config_type: Optional[str] = None

    def get_config(self) -> DatabricksConfig:
        profile_config = self._get_config_from_profile(self.DBX_PROFILE_ENV)
        env_config = self._get_config_from_env()

        if not profile_config and not env_config:
            raise Exception(
                "No valid authentication information was provided.\n"
                "Please either provide host and token for environment as per Databricks CLI docs \n"
                f"Or provide the env variable {self.DBX_PROFILE_ENV} which points to the pre-configured profile."
            )
        elif env_config:
            if profile_config:
                dbx_echo(
                    "Both profile and host/token environment variables were provided."
                    "dbx prioritises the host/token environment variables"
                )
            dbx_echo("host/token environment variables will be used")
            return env_config
        else:
            dbx_echo(f"profile with name {self.DBX_PROFILE_ENV} will be used")
            return profile_config
