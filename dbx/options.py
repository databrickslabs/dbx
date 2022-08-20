from pathlib import Path

import typer
from databricks_cli.configure.provider import DEFAULT_SECTION

from dbx.callbacks import (
    debug_callback,
    deployment_file_callback,
    verify_jinja_variables_file,
    execute_parameters_callback,
    launch_parameters_callback,
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

REQUIREMENTS_FILE_OPTION = typer.Option(
    Path("requirements.txt"),
    help="""
    [red bold]This option is deprecated.

    Please use setup.py or poetry for package management. [/red bold]
""",
)

NO_REBUILD_OPTION = typer.Option(
    False,
    "--no-rebuild",
    is_flag=True,
    help="""
    [red]This option is deprecated.

    Please use [code]build[/code] configuration section in the deployment file. [/red]
""",
)
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
    show_default=True,
    callback=execute_parameters_callback,
)

LAUNCH_PARAMETERS_OPTION = typer.Option(
    None,
    "--parameters",
    help="""If provided, overrides parameters of the chosen set of workflow(s), or task(s) inside workflow(s)

            Depending on the workflow or task type, it might contain various payloads.

            Provided payload shall match the expected payload of a chosen workflow or task.

            [bold]Please pass the parameters as described in the RunNow method of Jobs API v2.1.

            dbx will automatically convert them to a proper structure.[/bold]

            It also should be wrapped as a JSON-compatible string with curly brackets around API-compatible payload.

            Examples:

            - Jobs API v2.0 spark_python_task, or python_wheel_task workflow without tasks inside

            [bold]dbx launch <workflow_name> --parameters='{"parameters": ["argument1", "argument2"]}'[/bold]

            - Jobs API v2.1 multitask job with one python_wheel_task

            [bold]dbx execute <workflow_name> --parameters='[{"task_key": "some", "named_parameters":
            ["--a=1", "--b=2"]}]'[/bold]

            - Jobs API v2.1 multitask job with one notebook_task

            [bold]dbx execute <workflow_name> --parameters='[{"task_key": "some", "base_parameters":
            {"a": 1, "b": 2}}]'[/bold]

            - Jobs API v2.1 multitask job with 2 tasks

            [bold]dbx execute <workflow_name> --parameters='[

            {"task_key": "first", "base_parameters": {"a": 1, "b": 2}},

            {"task_key": "second", "parameters": ["a", "b"]}

            ]'[/bold]

            :rotating_light: [red bold]Please note that various tasks have various parameter structures.[/red bold]

            Also note that all parameters provided for the workflow or task will be preprocessed with file uploader.

            It means that if you reference a [bold]file://[/bold] or [bold]file:fuse://[/bold]
            in the parameter override, it will be resolved and uploaded to DBFS.

            You can find more on the parameter structures for various Jobs API
            versions in the official documentation""",
    show_default=True,
    callback=launch_parameters_callback,
)
