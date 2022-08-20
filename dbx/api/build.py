import shlex
import subprocess
import sys
from pathlib import Path
from typing import Union, List, Optional

from rich.console import Console

from dbx.models.deployment import BuildConfiguration, PythonBuild
from dbx.utils import dbx_echo


def cleanup_dist():
    dist_path = Path("dist")
    if dist_path.exists():
        dbx_echo("🧹 Standard package folder [bold]dist[/bold] already exists, cleaning it before Python package build")
        for _file in dist_path.glob("*.whl"):
            _file.unlink()


def execute_shell_command(
    cmd: Union[str, List[str]],
    with_python_executable: Optional[bool] = False,
):
    _cmd = shlex.split(cmd) if isinstance(cmd, str) else cmd
    _cmd = [sys.executable] + _cmd if with_python_executable else _cmd

    try:
        subprocess.check_output(_cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        dbx_echo("\n💥Command execution failed")
        raise exc


def prepare_build(build_config: BuildConfiguration):
    if build_config.no_build:
        dbx_echo("No build actions will be performed.")
    else:
        dbx_echo("Following the provided build logic")

        if build_config.commands:
            dbx_echo("Running the build commands")
            for command in build_config.commands:
                with Console().status(f"🔨Running command {command}", spinner="dots"):
                    execute_shell_command(command)
        elif build_config.python:
            dbx_echo("🐍 Building a Python-based project")
            cleanup_dist()

            if build_config.python == PythonBuild.poetry:
                command = "-m poetry build -f wheel"
            elif build_config.python == PythonBuild.flit:
                command = "-m flit build --format wheel"
            else:
                command = "-m pip wheel -w dist -e . --prefer-binary --no-deps"

            with Console().status("Building the package :hammer:", spinner="dots"):
                execute_shell_command(command, with_python_executable=True)
            dbx_echo(":white_check_mark: Python-based project build finished")
