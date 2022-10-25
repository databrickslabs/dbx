import shlex
import subprocess
import sys
from pathlib import Path
from typing import Union, List, Optional

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
