import json
from http import client as http_client
from pathlib import Path
from typing import Optional

import typer

from dbx import __version__
from dbx.models.cli.execute import ExecuteParametersPayload
from dbx.utils import dbx_echo


def verify_jinja_variables_file(_, value: Optional[Path]) -> Optional[Path]:
    if value:
        if value.suffix not in [".yaml", ".yml"]:
            raise Exception("Jinja variables file shall be provided in yaml or yml format")
        if not value.exists():
            raise FileNotFoundError(f"Jinja variables file option is not empty, but file is non-existent {value}")
        dbx_echo(f":ok: Jinja variables file {value} will be used for deployment")
        return value


def deployment_file_callback(_, value: Optional[str]) -> Path:
    if value:
        _candidate = Path(value)
        if not _candidate.exists():
            raise FileNotFoundError(f"Deployment file {_candidate} does not exist")
    else:
        dbx_echo(":mag_right: Deployment file is not provided, searching in the [red]conf[/red] directory")
        potential_extensions = ["json", "yml", "yaml", "json.j2", "yaml.j2", "yml.j2"]
        _candidate = None
        for ext in potential_extensions:
            candidate = Path(f"conf/deployment.{ext}")
            if candidate.exists():
                dbx_echo(f":bulb: Auto-discovery found deployment file {candidate}")
                _candidate = candidate
                break

        if not _candidate:
            raise FileNotFoundError(
                "Auto-discovery was unable to find any deployment file in the conf directory. "
                "Please provide file name via --deployment-file option"
            )

    dbx_echo(f":ok: Deployment file {_candidate} exists and will be used for deployment")
    return _candidate


def version_callback(value: bool):
    if value:
        dbx_echo(
            f":brick: [red]Databricks[/red] e[red]X[/red]tensions aka [red]dbx[/red], "
            f"version ~> [green]{__version__}[/green]"
        )
        raise typer.Exit()


def debug_callback(_, value):
    if value:
        dbx_echo(":bug: Debugging mode is [red]on[/red]")
        http_client.HTTPConnection.debuglevel = 1


def execute_parameters_callback(_, value: str) -> Optional[str]:
    if value:
        try:
            json.loads(value)
        except json.JSONDecodeError as e:
            dbx_echo(":boom: Provided parameters payload cannot be parsed since it's not in json format")
            raise e

        ExecuteParametersPayload.from_json(value)

        return value


def launch_parameters_callback(_, value: str) -> Optional[str]:
    if value:
        try:
            json.loads(value)
        except json.JSONDecodeError as e:
            dbx_echo(":boom: Provided parameters payload cannot be parsed as JSON")
            raise e

        return value
