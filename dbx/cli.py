import click
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.commands.configure import configure
from dbx.commands.deploy import deploy
from dbx.commands.execute import execute
from dbx.commands.launch import launch


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(message="DataBricks eXtensions aka dbx, version ~> %(version)s")
def cli():
    pass


cli.add_command(configure, name="configure")
cli.add_command(deploy, name="deploy")
cli.add_command(launch, name="launch")
cli.add_command(execute, name="execute")

if __name__ == "__main__":
    cli()
