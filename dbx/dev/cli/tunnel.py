from urllib.parse import urljoin

import click
from databricks_cli.configure.provider import DatabricksConfig
from databricks_cli.sdk import ClusterService
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.commands.execute import preprocess_cluster_args, awake_cluster, get_context_id, execute_command
from dbx.utils.common import environment_option, prepare_environment, get_environment_data, pick_config, dbx_echo, \
    ApiV1Client

DBX_TUNNEL_SERVER_PORT = 7681


@click.command(
    context_settings=CONTEXT_SETTINGS,
    short_help="Starts the tunnel to dbx dev server",
)
@click.option("--cluster-id", required=False, type=str, help="Cluster ID.")
@click.option("--cluster-name", required=False, type=str, help="Cluster name.")
@environment_option
def tunnel(
        environment: str,
        cluster_id: str,
        cluster_name: str,
):
    api_client = prepare_environment(environment)
    environment_data = get_environment_data(environment)
    _, config = pick_config(environment_data)

    cluster_id = preprocess_cluster_args(api_client, cluster_name, cluster_id)
    cluster_service = ClusterService(api_client)

    dbx_echo("Preparing cluster cluster to accept connections")
    awake_cluster(cluster_service, cluster_id)
    dbx_echo("Preparing cluster cluster to accept connections - done")

    v1_client = ApiV1Client(api_client)
    context_id = get_context_id(v1_client, cluster_id, "python")

    inspector_command = """
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    print(ctx.tags().get("orgId").get())
    """

    org_id = execute_command(
        v1_client, cluster_id, context_id, inspector_command, verbose=False
    )

    driver_app_url = _build_api_url(config, org_id, cluster_id)


def _build_api_url(config: DatabricksConfig, org_id: str, cluster_id: str) -> str:
    return urljoin(config.host, f"driver-proxy/o/{org_id}/{cluster_id}/{DBX_TUNNEL_SERVER_PORT}/")
