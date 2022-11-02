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

PROFILE_OPTION = typer.Option(
    DEFAULT_SECTION,
    "--profile",
    help="""Databricks CLI profile to use.


    Please note that this profile shall only be used for local development.


    For CI/CD pipelines please use `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables.""",
)
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
    help="""Path to deployment file.

    If not provided, auto-discovery will try to find relevant file in `conf` directory.""",
    callback=deployment_file_callback,
    show_default=False,
)

JINJA_VARIABLES_FILE_OPTION = typer.Option(
    None,
    "--jinja-variables-file",
    help="""Path to a file with variables for Jinja template.


        Only works when Jinja-based deployment file is used OR inplace Jinja support is enabled.


        ℹ️ Read more about this functionality in the Jinja2 support doc.""",
    callback=verify_jinja_variables_file,
)

REQUIREMENTS_FILE_OPTION = typer.Option(
    None,
    "--requirements-file",
    help="""
    This option is deprecated.

    Please use `setup.py` and other standard Python packaging and dependency management tools.
""",
)

NO_REBUILD_OPTION = typer.Option(
    False,
    "--no-rebuild",
    is_flag=True,
    help="""This option is deprecated. Please use `build` configuration section in the deployment file.""",
)
NO_PACKAGE_OPTION = typer.Option(
    False,
    "--no-package",
    is_flag=True,
    help="""Do not add default Python package reference into the workflow description.

    Useful for cases when pure Notebook or pure JVM workflows are deployed.""",
)

TAGS_OPTION = typer.Option(
    None,
    "--tags",
    help="""
    Additional tags for deployment.


    Please note that these tags are only relevant for the `artifact_location`,
    passing tags in the `dbx deploy` will not change the Tags section in the Job UI.


    Example: `--tags tag1=value1`


    Option might be repeated multiple times: `--tags tag1=value1 --tags tag2=value2`""",
)

BRANCH_NAME_OPTION = typer.Option(
    None,
    "--branch-name",
    help="""The name of the current branch. If not provided, dbx will try to detect the branch name.""",
)

WORKFLOW_ARGUMENT = typer.Argument(
    None,
    help="""Name of the workflow from the deployment file.
    Will raise an exception if provided together with `--workflows`.""",
)

EXECUTE_PARAMETERS_OPTION = typer.Option(
    None,
    "--parameters",
    help="""If provided, overrides parameters of the chosen workflow or a task inside workflow.

            Depending on the job or task type, it might contain various payloads.

            Provided payload shall match the expected payload of a chosen workflow or task.

            It also should be wrapped as a JSON-compatible string with curly brackets around API-compatible payload.

            Examples:


            dbx execute <workflow_name> --parameters='{parameters: ["argument1", "argument2"]}'

            dbx execute <workflow_name> --parameters='{named_parameters: ["--a=1", "--b=2"]}'


            Please note that various tasks have various parameter structures.

            Also note that all parameters provided for the workflow or task will be preprocessed with file uploader.

            It means that if you reference a `file://` in the parameter override, it will be resolved and
            uploaded to DBFS.

            You can find more on the parameter structures for various Jobs API
            versions in the official documentation""",
    show_default=True,
    callback=execute_parameters_callback,
)

LAUNCH_PARAMETERS_OPTION = typer.Option(
    None,
    "--parameters",
    help="""If provided, overrides parameters of the chosen workflow.


    Please read more details on the parameter passing in the dbx docs.
    """,
    show_default=True,
    callback=launch_parameters_callback,
)
