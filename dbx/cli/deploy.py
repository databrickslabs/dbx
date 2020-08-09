import click
import mlflow
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.cli.utils import InfoFile, dbx_echo, setup_mlflow, custom_profile_option, extract_version, build_project_whl, \
    upload_whl

"""
Logic behind this functionality:
0. Start mlflow run
1. Build a .whl file locally
2. Extract project version form .whl
3. Upload .whl to mlfow
"""


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Deploys project to artifact storage")
@click.option("--env-name", required=True, type=str, help="Environment name")
@debug_option
@custom_profile_option
@setup_mlflow
def deploy(env_name):
    """
    Deploys the project project
    """
    project_name = InfoFile.get("project_name")
    dbx_echo("Starting execution for project: %s" % project_name)

    with mlflow.start_run():
        dbx_echo("Building whl file")
        whl_file = build_project_whl()
        package_version = extract_version(whl_file)
        upload_whl(whl_file)

        tags = {
            "environment": env_name,
            "version": package_version,
            "action_type": "deploy"
        }

        mlflow.set_tags(tags)


