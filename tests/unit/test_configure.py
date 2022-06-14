import pathlib
from pathlib import Path

from dbx.api.configure import ProjectConfigurationManager
from dbx.commands.configure import configure
from dbx.constants import INFO_FILE_PATH
from conftest import invoke_cli_runner


def test_configure_default(temp_project: pathlib.Path):
    Path(INFO_FILE_PATH).unlink()
    first_result = invoke_cli_runner(
        configure,
        [
            "--environment",
            "test",
        ],
    )
    assert first_result.exit_code == 0
    env = ProjectConfigurationManager().get("test")
    assert env is not None
    assert env.properties.artifact_location == f"dbfs:/dbx/{temp_project.name}"
    assert env.properties.workspace_directory == f"/Shared/dbx/projects/{temp_project.name}"


def test_configure_with_parameters(temp_project: pathlib.Path):
    ws_dir = "/Shared/dbx/projects/%s" % temp_project.name
    artifact_location = f"dbfs:/dbx/custom-project-location/{temp_project.name}"
    first_result = invoke_cli_runner(
        configure,
        [
            "--environment",
            "test",
            "-p",
            f"workspace_directory={ws_dir}",
            "-p",
            f"artifact_location={artifact_location}",
        ],
    )

    assert first_result.exit_code == 0
    env = ProjectConfigurationManager().get("test")
    assert env is not None
    assert env.properties.workspace_directory == ws_dir
    assert env.properties.artifact_location == artifact_location


def test_update_configuration(temp_project: pathlib.Path):
    ws_dir = "/Shared/dbx/projects/%s" % temp_project.name
    invoke_cli_runner(
        configure,
        [
            "--environment",
            "test",
            "-p",
            f"workspace_directory={ws_dir}",
        ],
    )

    new_ws_dir = ws_dir + "/updated"
    invoke_cli_runner(
        configure,
        [
            "--environment",
            "test",
            "-p",
            f"workspace_directory={new_ws_dir}",
        ],
    )

    env = ProjectConfigurationManager().get("test")
    assert env is not None
    assert env.properties.workspace_directory, new_ws_dir
