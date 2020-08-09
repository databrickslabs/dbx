import click
from databricks_cli.configure.config import profile_option, debug_option
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.cli.clusters import create_dev_cluster, stop_dev_cluster
from dbx.cli.deploy import deploy
from dbx.cli.execute import execute
from dbx.cli.init import init
from dbx.cli.launch import launch


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(message='DataBricks eXtensions aka dbx, version ~> %(version)s')
@profile_option
@debug_option
def cli():
    pass


cli.add_command(init, name='init')
cli.add_command(create_dev_cluster, name="create-dev-cluster")
cli.add_command(stop_dev_cluster, name="stop-dev-cluster")
cli.add_command(execute, name="execute")
cli.add_command(deploy, name="deploy")
cli.add_command(launch, name="launch")

if __name__ == "__main__":
    cli()
