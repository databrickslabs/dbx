from __future__ import annotations

from enum import Enum
from typing import Optional, List

from pydantic import BaseModel
from rich.console import Console

from dbx.api.build import execute_shell_command, cleanup_dist
from dbx.utils import dbx_echo


class PythonBuild(str, Enum):
    pip = "pip"
    poetry = "poetry"
    flit = "flit"


class BuildConfiguration(BaseModel):
    no_build: Optional[bool] = False
    commands: Optional[List[str]]
    python: Optional[PythonBuild] = PythonBuild.pip

    def trigger_build_process(self):
        if self.no_build:
            dbx_echo("No build actions will be performed.")
        else:
            dbx_echo("Following the provided build logic")

        if self.commands:
            dbx_echo("Running the build commands")
            for command in self.commands:
                with Console().status(f"🔨Running command {command}", spinner="dots"):
                    execute_shell_command(command)
        elif self.python:
            dbx_echo("🐍 Building a Python-based project")
            cleanup_dist()

            if self.python == PythonBuild.poetry:
                build_kwargs = {"cmd": "poetry build -f wheel"}
            elif self.python == PythonBuild.flit:
                command = "-m flit build --format wheel"
                build_kwargs = {"cmd": command, "with_python_executable": True}
            else:
                command = "-m pip wheel -w dist -e . --prefer-binary --no-deps"
                build_kwargs = {"cmd": command, "with_python_executable": True}

            with Console().status("Building the package :hammer:", spinner="dots"):
                execute_shell_command(**build_kwargs)
            dbx_echo(":white_check_mark: Python-based project build finished")
        else:
            dbx_echo("Neither commands nor python building configuration was provided, skipping the build stage")
