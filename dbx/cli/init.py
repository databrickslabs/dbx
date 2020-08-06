import os

import click
from cookiecutter.main import cookiecutter

import dbx
from dbx.cli.constants import CONTEXT_SETTINGS

TEMPLATE_PATH = os.path.join(dbx.__path__[0], "template")


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Initializes a plain new project in a new directory.')
@click.option("--project-name", required=True, type=str)
@click.option('--cloud', required=True, type=click.Choice(['Azure', 'AWS'], case_sensitive=True))
@click.option('--pipeline-engine', required=True,
              type=click.Choice(['GitHub Actions', 'Azure Pipelines'], case_sensitive=True))
def init(**kwargs):
    """
    Initializes a plain new project in a new directory
    """
    cookiecutter(TEMPLATE_PATH, extra_context=kwargs, no_input=True)
