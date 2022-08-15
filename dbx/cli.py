import typer

from dbx.commands.configure import configure
from dbx.commands.datafactory import datafactory_app
from dbx.commands.deploy import deploy
from dbx.commands.execute import execute
from dbx.commands.init import init
from dbx.commands.launch import launch
from dbx.commands.sync import sync
from dbx.commands.version import version_entrypoint

app = typer.Typer(rich_markup_mode="rich")

app.callback()(version_entrypoint)

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

app.command(
    short_help=":fire: Executes chosen workload on the interactive cluster.",
    help="""
    :fire: Executes chosen workload on the interactive cluster.

    This command is very suitable to interactively execute your code on the interactive clusters.

    .. warning::

        There are some limitations for :code:`dbx execute`:

        * Only clusters which support :code:`%pip` magic can work with execute.
        * Currently, only Python-based execution is supported.

    The following set of actions will be done during execution:

    1. If interactive cluster is stopped, it will be automatically started
    2. Package will be rebuilt from the source (can be disabled via :option:`--no-rebuild`)
    3. Job configuration will be taken from deployment file for given environment
    4. All referenced will be uploaded to the MLflow experiment
    5. | Code will be executed in a separate context. Other users can work with the same package
       | on the same cluster without any limitations or overlapping.
    6. Execution results will be printed out in the shell. If result was an error, command will have error exit code.

    """,
)(execute)

app.command(
    short_help=":gem: Generates new project from the template",
    help="""
    :gem: Generates new project from the template

    Launching this command without :code:`--template-parameters` argument
    will open cookiecutter dialogue to enter the required parameters.
    """,
)(init)

app.command(
    short_help=":rocket: Launch the workload on a job cluster",
    help="""
    :rocket: Launch the workload on a job cluster

    This command will launch the given workload by it's name on a given environment.

    .. note::
        Workloads shall be deployed prior to be launched

    """,
)(launch)

typer_click_object = typer.main.get_command(app)

typer_click_object.add_command(sync, "sync")


def entrypoint():
    typer_click_object()
