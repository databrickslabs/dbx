import os
from typing import Optional
from functools import lru_cache
from databricks_cli.configure.provider import DatabricksConfig, ProfileConfigProvider, EnvironmentVariableConfigProvider

from dbx.utils import dbx_echo


class AuthConfigProvider:
    DBX_PROFILE_ENV = "DBX_CLI_PROFILE"

    @classmethod
    def _get_config_from_profile(cls) -> Optional[DatabricksConfig]:
        profile = cls._get_profile_name()
        if not profile:
            dbx_echo(
                f"Environment variable {cls.DBX_PROFILE_ENV} is not provided."
                f"Looking for host and token environment variables..."
            )
            return None
        else:
            config = ProfileConfigProvider(profile).get_config()
            if not config:
                raise Exception(f"Requested profile {profile} is not provided in ~/.databrickscfg")

            AuthConfigProvider._verify_config_validity(config)
            return config

    @staticmethod
    def _verify_config_validity(config: DatabricksConfig):
        if not config.is_valid_with_token:
            raise Exception(
                "Provided auth configuration is not based on token authentication."
                "Please switch to token-based authentication instead."
            )

    @staticmethod
    def _get_config_from_env() -> Optional[DatabricksConfig]:
        config = EnvironmentVariableConfigProvider().get_config()
        if config:
            AuthConfigProvider._verify_config_validity(config)
            return config

    @classmethod
    def _get_profile_name(cls) -> str:
        return os.environ.get(cls.DBX_PROFILE_ENV)

    @classmethod
    @lru_cache(maxsize=None)
    def get_config(cls) -> DatabricksConfig:
        profile_config = cls._get_config_from_profile()
        env_config = cls._get_config_from_env()

        if not profile_config and not env_config:
            raise Exception(
                "No valid authentication information was provided.\n"
                "Please either provide host and token for environment as per Databricks CLI docs \n"
                f"Or provide the env variable {cls.DBX_PROFILE_ENV} which points to the pre-configured profile."
            )

        if env_config:
            if profile_config:
                dbx_echo(
                    "Both profile and host/token environment variables were provided."
                    "dbx prioritises the host/token environment variables"
                )
            dbx_echo("Host/token environment variables will be used")
            return env_config
        else:
            dbx_echo(f"Profile {cls._get_profile_name()} will be used")
            return profile_config
