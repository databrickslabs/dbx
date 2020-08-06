import click

from dbx.cli.constants import CONTEXT_SETTINGS
from dbx.cli.init import init


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(message='DataBricks eXtensions aka dbx, version ~> %(version)s')
def cli():
    pass


cli.add_command(init, name='init')

if __name__ == "__main__":
    cli()
