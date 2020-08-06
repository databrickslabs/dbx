import click

from databrickslabs_cicdtemplates import __version__
from databrickslabs_cicdtemplates.constants import CONTEXT_SETTINGS
from databrickslabs_cicdtemplates.version import print_version_callback


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=__version__)
def cli():
    pass


if __name__ == "__main__":
    cli()
