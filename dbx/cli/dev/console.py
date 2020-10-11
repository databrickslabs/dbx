import os
import sys

import click
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.utils.common import (
    dbx_echo, ContextLockFile
)


def check_ngrok_env():
    if not os.environ.get("DBX_NGROK_TOKEN"):
        dbx_echo("""Current implementation of dbx dev supports only ngrok-based tunnels.
        please obtain a token from ngrok.com and set it as an environment variable:
            export DBX_NGROK_TOKEN='{your token here}' 
        """)
        sys.exit(1)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Launches dev console")
def console():
    dbx_echo("Starting dev console")
    tunnel_info = ContextLockFile.get_tunnel_info()
    if not tunnel_info:
        raise Exception("Tunnel info is not provided, please launch dbx dev watchdog to start the tunnel")
    else:
        os.system(f"ssh root@{tunnel_info.host} -i {tunnel_info.private_key_file} -p {tunnel_info.port}")
