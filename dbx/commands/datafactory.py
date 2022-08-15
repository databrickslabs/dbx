import logging
import sys
from pathlib import Path

import typer

from dbx.api.datafactory import DatafactoryReflector
from dbx.utils import dbx_echo
from dbx.options import ENVIRONMENT_OPTION


def filter_environment_credential_warning(record):
    if record.name.startswith("azure.identity") and record.levelno == logging.WARNING:
        message = record.getMessage()
        return not message.startswith("EnvironmentCredential.get_token")
    return True


handler = logging.StreamHandler(sys.stdout)
handler.addFilter(filter_environment_credential_warning)
logging.basicConfig(level=logging.ERROR, handlers=[handler])  # noqa

datafactory_app = typer.Typer(rich_markup_mode="rich")


@datafactory_app.command(help=":large_blue_diamond: Reflects workload definitions to Azure Data Factory.")
def reflect(
    specs_file: Path = typer.Option(
        ...,
        "--specs-file",
        exists=True,
        file_okay=True,
        dir_okay=False,
        help="Path to deployment result specification file.",
    ),
    subscription_name: str = typer.Option(..., "--subscription-name", help="Name of Azure subscription"),
    resource_group: str = typer.Option(..., "--resource-group", "-g", help="Resource group name"),
    factory_name: str = typer.Option(..., "--factory-name", help="Factory name"),
    name: str = typer.Option(..., "--name", "-n", help="Pipeline name"),
    environment: str = ENVIRONMENT_OPTION,
):
    dbx_echo("Reflecting job specifications to Azure Data Factory")
    reflector = DatafactoryReflector(specs_file, subscription_name, resource_group, factory_name, name, environment)
    reflector.launch()
