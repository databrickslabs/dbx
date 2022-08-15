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

    1. Python package will be built and stored in [bold]dist[/bold] folder.
       This behaviour can be disabled via [bold]--no-rebuild[/bold] option.
    2. Deployment configuration will be taken
       from the deployment file, defined in [bold]--deployment-file[/bold].
       You can specify the deployment file in either JSON or YAML or Jinja-based JSON or YAML.
       [bold][.json, .yaml, .yml, .j2][/bold] are all valid file types.
       If file is not provided, auto-discovery will try to find it in [bold]conf[/bold] directory.
    3. From the provided deployment file, the environment specified in option [bold]--environment[/bold] will be chosen.
    4. For this environment, the chosen set of workloads will be deployed.

       :warning: If you're using [bold]--jobs[/bold] or [bold]--job[/bold]
       please note that they're [red bold]deprecated[/red bold].

       There are 3 options to choose the deployable workloads:
       - Option #1: single name    [bold]dbx deploy workload-name[/bold]
       - Option #2: multiple names [bold]dbx deploy workload-name-1,workload-name-2[/bold]
       - Option #3: deploy all     [bold]dbx deploy --all[/bold]


    4. Any file references specified in the deployment file will be resolved.
       These files will be uploaded to the [bold]artifact_location[/bold] specified
       for the environment in [red].dbx/project.json[/red].
    5. If option [bold]--requirements-file[/bold] is provided, all requirements will be added to workload definition.
       [red bold]Please note that this option is deprecated[/red bold].
    6. Project wheel file will be added to the workload dependencies.
       This behaviour can be disabled via [bold]--no-package[/bold].
    7. If option [bold]--assets-only[/bold] or a deprecated [bold]--files-only[/bold] is provided,
       then assets will be uploaded to the [bold]artifact_location[/bold], but the job object won't be created/updated.
    8. If [bold]--write-specs-to-file[/bold] is provided, writes the final workload definition into a given file.
    """,
)(deploy)

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
