import pathlib

from conftest import invoke_cli_runner
from dbx.commands.deploy.deploy import deploy_job, deploy_snapshot


def test_job_all(temp_project: pathlib.Path, job_creation_fixture):
    invoke_cli_runner(deploy_job, ["--all"])


def test_snapshot_all(temp_project: pathlib.Path):
    invoke_cli_runner(deploy_snapshot, ["--all"])
