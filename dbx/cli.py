import click
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.commands.configure import configure
from dbx.commands.deploy import deploy
from dbx.commands.execute import execute
from dbx.commands.launch import launch
from dbx.commands.datafactory import datafactory
from dbx.commands.init import init
from dbx.commands.sync import sync


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(message="DataBricks eXtensions aka dbx, version ~> %(version)s")
def cli():
    pass


cli.add_command(configure, name="configure")
cli.add_command(deploy, name="deploy")
cli.add_command(launch, name="launch")
cli.add_command(execute, name="execute")
cli.add_command(datafactory, name="datafactory")
cli.add_command(init, name="init")
cli.add_command(sync, name="sync")

if __name__ == "__main__":
    cli()
