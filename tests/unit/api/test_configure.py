import shutil
from pathlib import Path
from unittest.mock import Mock

import pytest

from dbx.api import configure as test_module
from dbx.api.configure import ProjectConfigurationManager

TEST_MODULE_PATH = test_module.__name__


def test_configure_non_existent_project(temp_project: Path):
    shutil.rmtree(temp_project / ".dbx")
    with pytest.raises(FileNotFoundError):
        ProjectConfigurationManager().get("default")


def test_configure_jinja_support(temp_project: Path):
    _manager = ProjectConfigurationManager()
    _manager.enable_jinja_support()
    assert _manager.get_jinja_support()


@pytest.mark.parametrize(
    "param_name, value_in_project_info, value_in_cli, expected",
    [
        (list(test_module.DBX_CONFIGURE_DEFAULTS)[0], 1, 2, 2),
        ("not_exist_param_name", 1, 2, 1),
    ],
)
def test_update_project_info_by_cli_params(param_name, value_in_project_info, value_in_cli, expected):
    mock_project_info = Mock()
    setattr(mock_project_info, param_name, value_in_project_info)
    mock_typer_ctx = Mock(params={**test_module.DBX_CONFIGURE_DEFAULTS, **{param_name: value_in_cli}})
    test_module.JsonFileBasedManager._update_project_info_by_cli_params(mock_project_info, mock_typer_ctx)
    assert getattr(mock_project_info, param_name) == expected


@pytest.mark.parametrize("file_exists", [(True), (False)])
def test_get_param_value(file_exists, monkeypatch):
    mock_jfbm = test_module.JsonFileBasedManager()
    mock_jfbm._file = Mock()
    param_name = "foo"
    value_from_file = 1
    value_from_defaults = 2
    mock_typed = Mock()
    setattr(mock_typed, param_name, value_from_file)
    mock_jfbm._read_typed = Mock()
    mock_jfbm._read_typed.return_value = mock_typed
    monkeypatch.setitem(test_module.DBX_CONFIGURE_DEFAULTS, param_name, value_from_defaults)
    mock_jfbm._file.exists.return_value = file_exists
    result = mock_jfbm.get_param_value(param_name)
    if file_exists:
        assert result == value_from_file
    else:
        assert result == value_from_defaults
