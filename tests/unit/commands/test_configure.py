from pathlib import Path

from dbx.constants import PROJECT_INFO_FILE_PATH
from dbx.utils.common import ProjectConfigurationManager
from dbx.utils.json import JsonUtils
from tests.unit.conftest import invoke_cli_runner


def test_configure_default(temp_project) -> None:
    Path(PROJECT_INFO_FILE_PATH).unlink()
    first_result = invoke_cli_runner(
        ["configure", "--environment", "test", "--profile", temp_project.name],
    )
    assert first_result.exit_code == 0
    env = ProjectConfigurationManager().get("test")
    assert env is not None
    assert env.profile == temp_project.name
    assert env.properties.workspace_directory, f"dbfs:/dbx/{temp_project.name}"
    assert env.properties.artifact_location, f"/Shared/dbx/projects/{temp_project.name}"


def test_configure_and_enable_jinja(temp_project) -> None:
    Path(PROJECT_INFO_FILE_PATH).unlink()
    first_result = invoke_cli_runner(
        ["configure", "--environment", "dev", "--profile", temp_project.name],
    )
    invoke_cli_runner(["configure", "--enable-inplace-jinja-support"])
    assert first_result.exit_code == 0
    env = ProjectConfigurationManager().get("dev")
    assert env is not None
    assert ProjectConfigurationManager().get_jinja_support()


def test_configure_and_enable_failsafe_cluster_reuse(temp_project) -> None:
    Path(PROJECT_INFO_FILE_PATH).unlink()
    first_result = invoke_cli_runner(
        ["configure", "--environment", "dev", "--profile", temp_project.name],
    )
    invoke_cli_runner(["configure", "--enable-failsafe-cluster-reuse-with-assets"])
    assert first_result.exit_code == 0
    env = ProjectConfigurationManager().get("dev")
    assert env is not None
    assert ProjectConfigurationManager().get_failsafe_cluster_reuse()


def test_configure_custom_location(temp_project: Path):
    ws_dir = "/Shared/dbx/projects/%s" % temp_project.name
    first_result = invoke_cli_runner(
        [
            "configure",
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

    env = ProjectConfigurationManager().get("test")
    assert env is not None
    assert env.profile == temp_project.name
    assert env.properties.workspace_directory == ws_dir


def test_config_update(temp_project: Path):
    ws_dir = "/Shared/dbx/projects/%s" % temp_project.name
    invoke_cli_runner(
        [
            "configure",
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
        [
            "configure",
            "--environment",
            "test",
            "--profile",
            temp_project.name,
            "--workspace-dir",
            new_ws_dir,
        ],
    )

    env = ProjectConfigurationManager().get("test")
    assert env is not None
    assert env.profile == temp_project.name
    assert env.properties.workspace_directory == new_ws_dir


def test_configure_from_legacy(temp_project):
    w_dir_legacy = "/Shared/dbx/some-dir"
    art_loc_legacy = "dbfs:/Shared/dbx/projects/some-dir"
    profile_legacy = "default"

    w_dir_new = "/Shared/dbx/some-dir-mod"
    art_loc_new = "dbfs:/Shared/dbx/projects/some-dir-new"
    profile_new = "new"

    _legacy = {
        "environments": {
            "legacy": {"profile": profile_legacy, "workspace_dir": w_dir_legacy, "artifact_location": art_loc_legacy},
            "new": {
                "profile": profile_new,
                "storage_type": "mlflow",
                "properties": {"workspace_directory": w_dir_new, "artifact_location": art_loc_new},
            },
        }
    }
    JsonUtils.write(PROJECT_INFO_FILE_PATH, _legacy)
    _env = ProjectConfigurationManager().get("legacy")
    assert _env.profile == profile_legacy
    assert _env.properties.workspace_directory == w_dir_legacy
    assert _env.properties.artifact_location == art_loc_legacy

    _env = ProjectConfigurationManager().get("new")
    assert _env.profile == profile_new
    assert _env.properties.workspace_directory == w_dir_new
    assert _env.properties.artifact_location == art_loc_new
