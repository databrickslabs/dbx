import click

from databrickslabs_cicdtemplates.cli.constants import CONTEXT_SETTINGS
from databrickslabs_cicdtemplates.cli.init import init


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(message='DataBricks eXtensions, version ~> %(version)s')
def cli():
    pass


cli.add_command(init, name='init')

if __name__ == "__main__":
    cli()
