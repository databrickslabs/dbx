# import click
# from databricks_cli.utils import CONTEXT_SETTINGS
#
# from dbx.commands.configure import configure
# from dbx.commands.deploy import deploy
# from dbx.commands.execute import execute
# from dbx.commands.launch import launch
# from dbx.commands.datafactory import datafactory
# from dbx.commands.init import init
# from dbx.commands.sync import sync

import typer

from dbx.commands.version import version_entrypoint
from dbx.utils._typer import add_callback
from dbx.commands.configure import configure
from dbx.commands.datafactory import datafactory_app
from dbx.commands.deploy import deploy

app = typer.Typer(rich_markup_mode="rich")

add_callback(app, version_entrypoint)

app.command(
    short_help=":wrench: Configures project environment in the current folder.",
    help="""
    :wrench: Configures project environment in the current folder.

    This command might be used multiple times to change configuration or add new environment.
    If project file (located in [red].dbx/project.json[/red]) is non-existent, it will be initialized.
    There is no strict requirement to configure project file via this command.
    You can also make changes in the project file via any file editor.
    """,
)(configure)

app.add_typer(datafactory_app, name="datafactory", help=":blue_heart: Azure Data Factory integration utilities.")

app.command(
    short_help=":hammer: Deploy project to artifact storage.",
    help="""
    :hammer: Deploy project to artifact storage.

    This command performs deployment to the given environment.

    During the deployment, following actions will be performed:

    1. Python package will be built and stored in :code:`dist/*` folder (can be disabled via :option:`--no-rebuild`)
    2. | Deployment configuration will be taken for a given environment (see :option:`-e` for details)
       | from the deployment file, defined in  :option:`--deployment-file`.
       | You can specify the deployment file in either JSON or YAML or Jinja-based JSON or YAML.
       | :code:`[.json, .yaml, .yml, .j2]` are all valid file types.
    3. Per each job defined in the :option:`--jobs`, all local file references will be checked
    4. Any found file references will be uploaded to MLflow as artifacts of current deployment run
    5. [DEPRECATED] If :option:`--requirements-file` is provided, all requirements will be added to job definition
    6. Wheel file location will be added to the :code:`libraries`. Can be disabled with :option:`--no-package`.
    7. If the job with given name exists, it will be updated, if not - created
    8. | If :option:`--write-specs-to-file` is provided, writes final job spec into a given file.
       | For example, this option can look like this: :code:`--write-specs-to-file=.dbx/deployment-result.json`.
    """,
)(deploy)
#
#
# cli.add_command(configure, name="configure")
# cli.add_command(deploy, name="deploy")
# cli.add_command(launch, name="launch")
# cli.add_command(execute, name="execute")
# cli.add_command(datafactory, name="datafactory")
# cli.add_command(init, name="init")
# cli.add_command(sync, name="sync")
#
# if __name__ == "__main__":
#     cli()
