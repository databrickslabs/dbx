from pathlib import Path

import typer
from databricks_cli.configure.provider import DEFAULT_SECTION

from dbx.callbacks import (
    debug_callback,
    deployment_file_callback,
    verify_jinja_variables_file,
    execute_parameters_callback,
)

ENVIRONMENT_OPTION = typer.Option("default", "--environment", "-e", help="Environment name.")

PROFILE_OPTION = typer.Option(DEFAULT_SECTION, "--profile", help="Databricks CLI profile to use.")
DEBUG_OPTION = typer.Option(
    False,
    "--debug",
    is_flag=True,
    callback=debug_callback,
    help="""
    Enable debugging of HTTP calls.

    Disabled by default.""",
)
DEPLOYMENT_FILE_OPTION = typer.Option(
    None,
    "--deployment-file",
    help="Path to deployment file. "
    "If not provided, auto-discovery will try to find relevant file in [green]conf[/green] directory.",
    callback=deployment_file_callback,
    show_default=False,
)

JINJA_VARIABLES_FILE_OPTION = typer.Option(
    None,
    "--jinja-variables-file",
    help="""
        Path to a file with variables for Jinja template.

        Only works when Jinja-based deployment file is used.

        :information_source: Read more about this functionality in the Jinja2 support doc.
        """,
    callback=verify_jinja_variables_file,
)

REQUIREMENTS_FILE_OPTION = typer.Option(Path("requirements.txt"), help="[red]This option is deprecated[/red]")

NO_REBUILD_OPTION = typer.Option(False, "--no-rebuild", is_flag=True, help="Disable package rebuild")
NO_PACKAGE_OPTION = typer.Option(
    False, "--no-package", is_flag=True, help="Do not add package reference into the job description"
)

TAGS_OPTION = typer.Option(
    None,
    "--tags",
    help="""Additional tags for deployment.

              Example: [bold]--tags tag1=value1[/bold].

              Option might be repeated multiple times: [bold]--tags tag1=value1 --tags tag2=value2[/bold]""",
)

BRANCH_NAME_OPTION = typer.Option(
    None,
    "--branch-name",
    help="""The name of the current branch.
              If not provided or empty, dbx will try to detect the branch name.""",
)

WORKFLOW_ARGUMENT = typer.Argument(
    None,
    help="""Name of the workflow from the deployment file.

            If this argument is provided, --job and --jobs arguments will be [red bold]ignored[/red bold]""",
)

EXECUTE_PARAMETERS_OPTION = typer.Option(
    None,
    "--parameters",
    help="""If provided, overrides parameters of the chosen workflow
            or a task inside workflow.

            Depending on the job or task type, it might contain various payloads.

            Provided payload shall match the expected payload of a chosen workflow or task.

            It also should be wrapped as a JSON-compatible string with curly brackets around API-compatible payload.

            Examples:

            [bold]dbx execute <workflow_name> --parameters='{parameters: ["argument1", "argument2"]}'[/bold]

            [bold]dbx execute <workflow_name> --parameters='{named_parameters: ["--a=1", "--b=2"]}'[/bold]

            :rotating_light: [red bold]Please note that various tasks have various parameter structures.[/red bold]

            Also note that all parameters provided for the workflow or task will be preprocessed with file uploader.

            It means that if you reference a [bold]file://[/bold] in the parameter override, it will be resolved and
            uploaded to DBFS.

            You can find more on the parameter structures for various Jobs API
            versions in the official documentation""",
    show_default=False,
    show_choices=False,
    callback=execute_parameters_callback,
)
