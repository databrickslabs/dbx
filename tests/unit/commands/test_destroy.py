from functools import partial
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import typer
from pytest_mock import MockerFixture

from dbx.api.config_reader import ConfigReader
from dbx.api.destroyer import Destroyer
from dbx.commands.destroy import ask_for_confirmation, parse_multiple
from dbx.models.cli.destroyer import DestroyerConfig, DeletionMode
from tests.unit.conftest import invoke_cli_runner


@pytest.fixture(scope="function")
def base_config(temp_project):
    config_reader = ConfigReader(Path("conf/deployment.yml"), None)
    config = config_reader.get_config()
    deployment = config.get_environment("default", raise_if_not_found=True)
    wfs = deployment.payload.select_relevant_or_all_workflows()
    return partial(DestroyerConfig, dracarys=False, deployment=deployment, workflows=wfs)


def test_ask_for_confirmation_positive(monkeypatch, base_config):
    monkeypatch.setattr("builtins.input", lambda: "yes")
    ask_for_confirmation(base_config(deletion_mode=DeletionMode.workflows_only))


def test_ask_for_confirmation_negative(monkeypatch, base_config):
    with pytest.raises(typer.Exit):
        monkeypatch.setattr("builtins.input", lambda: "no")
        ask_for_confirmation(base_config(deletion_mode=DeletionMode.workflows_only))


@pytest.mark.parametrize(
    "mode, expected",
    [
        ("all", "All assets are also marked for deletion"),
        ("workflows_only", "assets won't be affected"),
        ("assets_only", "workflow definitions won't be affected"),
    ],
)
def test_various_confirmation_inputs(mode, expected, base_config, capsys, monkeypatch):
    _mode = DeletionMode[mode]
    _c = base_config(deletion_mode=_mode)
    monkeypatch.setattr("builtins.input", lambda: "yes")
    ask_for_confirmation(_c)
    result = capsys.readouterr()
    assert expected in result.out.replace("\n", "")


def test_destroy_wrong_args(temp_project):
    with pytest.raises(Exception):
        invoke_cli_runner("destroy wf1 --workflows=wf2,wf3")


def test_destroy_smoke(mocker: MockerFixture, temp_project, monkeypatch):
    mocker.patch("dbx.commands.destroy.prepare_environment", MagicMock())
    launch_mock = mocker.patch.object(Destroyer, "launch", MagicMock())
    monkeypatch.setattr("builtins.input", lambda: "yes")
    invoke_cli_runner("destroy")
    launch_mock.assert_called_once()


def test_destroy_smoke_confirm(mocker: MockerFixture, temp_project, monkeypatch):
    mocker.patch("dbx.commands.destroy.prepare_environment", MagicMock())
    launch_mock = mocker.patch.object(Destroyer, "launch", MagicMock())
    invoke_cli_runner("destroy --confirm")
    launch_mock.assert_called_once()


def test_destroy_smoke_dry(mocker: MockerFixture, temp_project, monkeypatch, capsys):
    mocker.patch("dbx.commands.destroy.prepare_environment", MagicMock())
    launch_mock = mocker.patch.object(Destroyer, "launch", MagicMock())
    res = invoke_cli_runner("destroy --dry-run")
    launch_mock.assert_called_once()
    assert "Omitting the confirmation check" in res.stdout.replace("\n", "")


@pytest.mark.usefixtures("temp_project")
@pytest.mark.parametrize("h_option", ["-H", "--header"])
def test_asset_eraser_additional_headers(h_option, mocker: MockerFixture, monkeypatch):
    expected_headers = {
        "azure_sp_token": "eyJhbAAAABBBB",
        "workspace_id": (
            "/subscriptions/bc5bAAA-BBBB/resourceGroups/some-resource-group"
            "/providers/Microsoft.Databricks/workspaces/target-dtb-ws"
        ),
        "org_id": "1928374655647382",
    }
    env_mock = mocker.patch("dbx.commands.destroy.prepare_environment", MagicMock())
    header_parse_mock = mocker.patch("dbx.commands.destroy.parse_multiple", wraps=parse_multiple)
    launch_mock = mocker.patch.object(Destroyer, "launch", MagicMock())
    kwargs = [f"{key}={val}" for key, val in expected_headers.items()]
    cli_kwargs = " ".join([f"{h_option} {kw}" for kw in kwargs])
    monkeypatch.setattr("builtins.input", lambda: "yes")
    invoke_cli_runner(f"destroy {cli_kwargs}")
    launch_mock.assert_called_once()
    header_parse_mock.assert_called_once_with(kwargs)
    env_mock.assert_called_once_with("default", expected_headers)
