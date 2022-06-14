import pathlib

from conftest import invoke_cli_runner
from dbx.commands.deploy.deploy import deploy_job


def test_args_job(temp_project: pathlib.Path):
    invoke_cli_runner(deploy_job, ["--all"])
