from dbx.commands.configure import configure
from dbx.utils.common import ConfigurationManager
from dbx.constants import INFO_FILE_PATH
from pathlib import Path
from .conftest import invoke_cli_runner


def test_configure_default(temp_project) -> None:
    Path(INFO_FILE_PATH).unlink()
    first_result = invoke_cli_runner(
        configure,
        ["--environment", "test", "--profile", temp_project.name],
    )
    assert first_result.exit_code == 0
    env = ConfigurationManager().get("test")
    assert env is not None
    assert env.profile == temp_project.name
    assert env.artifact_location, f"dbfs:/dbx/{temp_project.name}"
    assert env.workspace_dir, f"/Shared/dbx/projects/{temp_project.name}"


def test_configure_custom_location(temp_project: Path):
    ws_dir = "/Shared/dbx/projects/%s" % temp_project.name
    first_result = invoke_cli_runner(
        configure,
        [
            "--environment",
            "test",
            "--profile",
            temp_project.name,
            "--workspace-dir",
            ws_dir,
            "--artifact-location",
            f"dbfs:/dbx/custom-project-location/{temp_project.name}",
        ],
    )

    assert first_result.exit_code == 0

    env = ConfigurationManager().get("test")
    assert env is not None
    assert env.profile == temp_project.name
    assert env.workspace_dir == ws_dir


def test_config_update(temp_project: Path):
    ws_dir = "/Shared/dbx/projects/%s" % temp_project.name
    invoke_cli_runner(
        configure,
        [
            "--environment",
            "test",
            "--profile",
            temp_project.name,
            "--workspace-dir",
            ws_dir,
        ],
    )

    new_ws_dir = ws_dir + "/updated"
    invoke_cli_runner(
        configure,
        [
            "--environment",
            "test",
            "--profile",
            temp_project.name,
            "--workspace-dir",
            new_ws_dir,
        ],
    )

    env = ConfigurationManager().get("test")
    assert env is not None
    assert env.profile == temp_project.name
    assert env.workspace_dir == new_ws_dir
