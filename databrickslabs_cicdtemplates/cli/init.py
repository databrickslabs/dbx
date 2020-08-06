import click
from cookiecutter.main import cookiecutter

from databrickslabs_cicdtemplates.cli.constants import CONTEXT_SETTINGS

TEMPLATE_URI = "git@github.com:databrickslabs/cicd-templates.git"


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Initializes a plain new project in a new directory.')
@click.option("--project-name", required=True, type=str)
@click.option("--version", required=False, type=str, default="0.0.1")
@click.option("--description", required=False, type=str, default="Project description")
@click.option("--author", required=True, type=str)
@click.option("--license", required=False, type=str, default="")
@click.option("--mlflow-experiment-path", required=False, type=str, default="/Shared/cicd")
@click.option('--cloud', required=True, type=click.Choice(['Azure', 'AWS'], case_sensitive=True))
@click.option('--cicd_tool', required=True,
              type=click.Choice(['GitHub Actions', 'Azure DevOps'], case_sensitive=True))
def init(**kwargs):
    """
    Initializes a plain new project in a new directory
    """
    cookiecutter(TEMPLATE_URI, extra_context=kwargs, no_input=True)
