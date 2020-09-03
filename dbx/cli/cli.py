import click
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.cli.configure import configure
from dbx.cli.deploy import deploy
from dbx.cli.execute import execute
from dbx.cli.launch import launch


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(message='DataBricks eXtensions aka dbx, version ~> %(version)s')
@debug_option
def cli():
    pass


cli.add_command(configure, name="configure")
cli.add_command(deploy, name="deploy")
cli.add_command(launch, name="launch")
cli.add_command(execute, name="execute")

if __name__ == "__main__":
    cli()
