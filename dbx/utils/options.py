from pathlib import Path

import click
from databricks_cli.configure.provider import DEFAULT_SECTION


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
