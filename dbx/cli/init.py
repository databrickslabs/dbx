import os

import click
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS
from path import Path
import shutil
import dbx
from dbx.cli.utils import InfoFile, dbx_echo

DEPLOYMENT_TEMPLATE_PATH = os.path.join(dbx.__path__[0], "template", "deployment.jsonnet")


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Initializes a plain new project in a new directory.')
@click.option("--project-name", required=True, type=str,
              help="Name of a project. Used to create an MLflow experiment.")
@click.option("--project-local-dir", required=False, type=str, default=".",
              help="Where to store generated project files.")
@click.option("--with-deployment", is_flag=True, help="Provide deployment.jsonnet sample file.")
@debug_option
def init(project_name: str, project_local_dir: str, with_deployment: bool):
    dbx_echo("Initializing project %s in directory: %s" % (project_name, project_local_dir))

    with Path(project_local_dir):
        os.mkdir(".dbx")

        InfoFile.initialize({
            "project_name": project_name,
            "environments": {}
        })

        if with_deployment:
            shutil.copy(DEPLOYMENT_TEMPLATE_PATH, ".dbx/deployment.jsonnet")

        dbx_echo("Project initialization finished")
