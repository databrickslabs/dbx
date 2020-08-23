import click
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS
from path import Path

from dbx.cli.utils import InfoFile, dbx_echo


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Initializes a plain new project in a new directory')
@click.option("--project-name", required=True, type=str,
              help="Name of a project. Used to create an MLflow experiment")
@click.option("--project-local-dir", required=False, type=str, default=".",
              help="Where to store generated project files")
@debug_option
def init(
        project_name: str,
        project_local_dir: str):
    dbx_echo("Initializing project %s in directory: %s" % (project_name, project_local_dir))

    with Path(project_local_dir):

        InfoFile.initialize({
            "project_name": project_name,
            "environments": {}
        })

        dbx_echo("Project initialization finished")
