from pathlib import Path

import typer
from databricks_cli.configure.provider import DEFAULT_SECTION

from dbx.callbacks import debug_callback, deployment_file_callback, verify_jinja_variables_file

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
