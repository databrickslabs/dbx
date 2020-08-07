import click
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.cli.utils import provide_lockfile_controller, LockFileController, read_json

DEV_CLUSTER_FILE = "config/dev/cluster.json"


@click.command(context_settings=CONTEXT_SETTINGS, short_help='Creates a dev cluster.')
@debug_option
@profile_option
@provide_api_client
@provide_lockfile_controller
def create_dev_cluster(api_client, lockfile_controller: LockFileController):
    """
    Creates a dev cluster for the project. Cluster id will be written into .dbx.lock.json file.
    """
    if lockfile_controller.get_dev_cluster_id():
        click.echo("[dbx] Cluster is already created, happy development!")
    else:
        click.echo("[dbx] Creating new development cluster")
        cluster_api = ClusterApi(api_client)
        cluster_config = read_json(DEV_CLUSTER_FILE)
        cluster_id = cluster_api.create_cluster(cluster_config).get("cluster_id")
        lockfile_controller.update({"dev_cluster_id": cluster_id})
        click.echo("[dbx] New cluster for development created")


@click.command(context_settings=CONTEXT_SETTINGS, short_help="Stops dev cluster.")
@debug_option
@profile_option
@provide_api_client
@provide_lockfile_controller
def stop_dev_cluster(api_client, lockfile_controller: LockFileController):
    """
    Stops the dev cluster. Cluster id will is taken from .dbx.lock.json file.
    """
    if not lockfile_controller.get_dev_cluster_id():
        raise click.exceptions.UsageError(
            "Couldn't stop the dev cluster as the cluster wasn't created under control of dbx")
    else:
        cluster_api = ClusterApi(api_client)
        cluster_id = lockfile_controller.get_dev_cluster_id()
        cluster_api.delete_cluster(cluster_id)
