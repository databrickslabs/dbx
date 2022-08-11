from pathlib import Path

import click
from databricks_cli.configure.provider import DEFAULT_SECTION

from dbx.callbacks import verify_jinja_variables_file


def environment_option(f):
    return click.option(
        "-e",
        "--environment",
        required=False,
        default="default",
        help="""Environment name. \n
            If not provided, :code:`default` will be used.""",
    )(f)


def profile_option(f):
    return click.option(
        "--profile",
        required=False,
        default=DEFAULT_SECTION,
        help="""CLI connection profile to use.\n
             The default profile is :code:`DEFAULT`.""",
    )(f)


def deployment_file_option(f):
    return click.option(
        "--deployment-file",
        required=False,
        type=click.Path(path_type=Path),
        help="Path to deployment file.",
        is_eager=True,
    )(f)


def jinja_variables_file_option(f):
    return click.option(
        "--jinja-variables-file",
        type=click.Path(path_type=Path),
        default=None,
        required=False,
        help="""
        Path to a file with variables for Jinja template. Only works when Jinja-based deployment file is used.
        Read more about this functionality in the Jinja2 support doc.
        """,
        callback=verify_jinja_variables_file,
    )(f)
