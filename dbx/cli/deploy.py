from copy import deepcopy

import click
import mlflow
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.cli.utils import dbx_echo, setup_mlflow, custom_profile_option, parse_tags, InfoFile, build_project_whl, \
    upload_whl, extract_version

"""
Logic behind this functionality:
0. Start mlflow run
1. Build a .whl file locally
2. Extract project version form .whl
3. Upload .whl to mlfow
"""

adopted_context = deepcopy(CONTEXT_SETTINGS)

adopted_context.update(dict(
    ignore_unknown_options=True,
))


@click.command(context_settings=adopted_context,
               short_help="Deploys project to artifact storage with given tags")
@click.argument('tags', nargs=-1, type=click.UNPROCESSED)
@debug_option
@custom_profile_option
@setup_mlflow
def deploy(tags):
    """
    Deploys the project. Please provide tags in format: --tag1=value1 --tag2=value2
    """
    deployment_tags = parse_tags(tags)
    project_name = InfoFile.get("project_name")
    dbx_echo("Starting deployment for project: %s with tags %s" % (project_name, deployment_tags))

    with mlflow.start_run():
        dbx_echo("Building whl file")
        whl_file = build_project_whl()
        package_version = extract_version(whl_file)
        upload_whl(whl_file)

        deployment_tags.update({
            "version": package_version,
            "action_type": "deploy"
        })

        mlflow.set_tags(deployment_tags)
