import sys

import click
import databricks_cli
import requests
from databricks_cli.configure.provider import DatabricksConfig, ProfileConfigProvider, get_config

from .clients import get_headers


def has_valid_token(config: DatabricksConfig) -> bool:
    """Check that the Databricks config has a valid token that works with the host.

    Args:
        config (DatabricksConfig): config with token and host

    Returns:
        bool: True if token can be used to call API, and False otherwise
    """
    token = config.token
    host = config.host.rstrip("/")
    headers = get_headers(token)
    resp = requests.get(f"{host}/api/2.0/dbfs/list", json={"path": "/"}, headers=headers)
    return resp.status_code == 200


def get_databricks_config(profile: str = None) -> DatabricksConfig:
    """
    Returns the Databricks config with the token and host for accessing Databricks while also validating that
    the token works with that host.

    The config is provided by classes in the Databricks CLI.
    """

    try:
        if profile:
            config = ProfileConfigProvider(profile).get_config()
        else:
            config = get_config()
    except databricks_cli.utils.InvalidConfigurationError as e:

        # If the Databricks CLI hasn't been configured yet, it will produce an error message.  But the error
        # message is confusing because it says we need to run `dbx configure`, rather than
        # `databricks configure`.  Databricks CLI code makes the reasonable assumption that sys.argv[0] is the
        # program to run, as it's usually called within its own CLI.
        # We correct this be replacing it with "databricks".
        msg = str(e)
        msg = msg.replace(sys.argv[0], "databricks")
        msg = msg.replace("the CLI yet", "the Databricks CLI yet")

        raise click.UsageError(msg)

    profile_str = f" for profile {profile}" if profile else ""

    if not config:
        raise click.UsageError(f"Could not find a databricks-cli config{profile_str}")

    if not has_valid_token(config):
        raise click.UsageError(f"Invalid token found for databricks-cli config{profile_str}")

    return config
