import functools
import pathlib
from typing import Callable

import click


def common_deploy_options(func: Callable) -> Callable:
    @click.argument("workload-name", required=False, type=str)
    @click.option(
        "--deployment-file",
        required=False,
        type=click.Path(exists=True, path_type=pathlib.Path),
        help="Path to the file with deployment definitions",
    )
    @click.option("--no-rebuild", is_flag=True, help="Disable package rebuild")
    @click.option(
        "--all",
        is_flag=True,
        help="If provided, all workloads defined for the given environment will be deployed.",
    )
    @click.option(
        "--tags",
        multiple=True,
        type=str,
        help="""Additional tags for deployment in format (tag_name=tag_value).
                  Option might be repeated multiple times.""",
    )
    @click.option(
        "--save-final-definitions",
        type=click.Path(exists=True, path_type=pathlib.Path),
        default=None,
        help="""Saves final workload definitions into a given local file.
                  Helpful when final representation of a deployed workloads is needed for other integrations.
                  Please note that output file will be overwritten if it exists.""",
    )
    @click.option(
        "--branch-name",
        type=str,
        default=None,
        required=False,
        help="""The name of the current branch.
                  If not provided or empty, dbx will try to detect the branch name.""",
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper
