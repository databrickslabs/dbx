from dbx.cli.dev.watchdog import watchdog
import click
from databricks_cli.utils import CONTEXT_SETTINGS


@click.group(context_settings=CONTEXT_SETTINGS, short_help='Development utilities for dbx')
def dev_group():
    pass


dev_group.add_command(watchdog, 'watchdog')
