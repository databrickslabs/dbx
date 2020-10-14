import click
from databricks_cli.cli import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS
import sys
import os
from dbx.cli.execute import _preprocess_cluster_args
from dbx.utils.common import (
    environment_option, dbx_echo, prepare_environment, ApiV1Client
)
from dbx.utils.watchdog.cluster_manager import ClusterManager
from dbx.utils.watchdog.context_manager import ContextManager
from dbx.utils.watchdog.dev_app import DevApp
from dbx.utils.watchdog.tunnel_manager import TunnelManager


def check_ngrok_env():
    if not os.environ.get("DBX_NGROK_TOKEN"):
        dbx_echo("""Current implementation of dbx dev supports only ngrok-based tunnels.
        please obtain a token from ngrok.com and set it as an environment variable:
            export DBX_NGROK_TOKEN='{your token here}' 
        """)
        sys.exit(1)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Launches local development appliance")
@click.option("--cluster-id", required=False, type=str, help="Cluster ID.")
@click.option("--cluster-name", required=False, type=str, help="Cluster name.")
@environment_option
@debug_option
def watchdog(environment: str,
             cluster_id: str,
             cluster_name: str):
    check_ngrok_env()
    dbx_echo("Starting watchdog in environment %s" % environment)

    api_client = prepare_environment(environment)
    api_v1_client = ApiV1Client(api_client)

    cluster_id = _preprocess_cluster_args(api_client, cluster_name, cluster_id)

    cluster_manager = ClusterManager(api_client, cluster_id)
    context_manager = ContextManager(api_v1_client, cluster_manager)
    tunnel_manager = TunnelManager(api_v1_client, cluster_id, context_manager)
    app = DevApp(environment, cluster_manager, context_manager, tunnel_manager)
    app.launch()
