from unittest.mock import MagicMock, call, patch

import click
import databricks_cli
import pytest

from dbx.sync.config import has_valid_token, get_databricks_config
from tests.unit.sync.utils import mocked_props


@patch("dbx.sync.config.get_headers")
@patch("dbx.sync.config.requests")
def test_has_valid_token(mock_requests, mock_get_headers):
    fake_headers = {"foo": "bar"}
    mock_get_headers.return_value = fake_headers
    resp = mocked_props(status_code=200)
    mock_requests.get.return_value = resp
    config = mocked_props(token="fake-token", host="http://somewhere.asdf/fake/")
    assert has_valid_token(config)

    assert mock_requests.get.call_args == call(
        "http://somewhere.asdf/fake/api/2.0/dbfs/list", json={"path": "/"}, headers=fake_headers
    )


@patch("dbx.sync.config.get_headers")
@patch("dbx.sync.config.requests")
def test_not_has_valid_token(mock_requests, mock_get_headers):
    fake_headers = {"foo": "bar"}
    mock_get_headers.return_value = fake_headers
    resp = mocked_props(status_code=401)
    mock_requests.get.return_value = resp
    config = mocked_props(token="fake-token", host="http://somewhere.asdf/fake/")
    assert not has_valid_token(config)


@patch("dbx.sync.config.has_valid_token")
@patch("dbx.sync.config.get_config")
def test_get_databricks_config_default(mock_get_config, mock_has_valid_token):
    config = MagicMock()
    mock_get_config.return_value = config
    mock_has_valid_token.return_value = True
    assert get_databricks_config() == config
    assert mock_get_config.call_count == 1
    assert mock_has_valid_token.call_args == call(config)


@patch("dbx.sync.config.has_valid_token")
@patch("dbx.sync.config.get_config")
def test_get_databricks_config_default_invalid_token(mock_get_config, mock_has_valid_token):
    config = MagicMock()
    mock_get_config.return_value = config
    mock_has_valid_token.return_value = False
    with pytest.raises(click.UsageError):
        get_databricks_config()
    assert mock_get_config.call_count == 1
    assert mock_has_valid_token.call_args == call(config)


@patch("dbx.sync.config.has_valid_token")
@patch("dbx.sync.config.get_config")
def test_get_databricks_config_default_no_config(mock_get_config, mock_has_valid_token):
    mock_get_config.return_value = None
    mock_has_valid_token.return_value = False
    with pytest.raises(click.UsageError):
        get_databricks_config()
    assert mock_get_config.call_count == 1
    assert mock_has_valid_token.call_count == 0


@patch("dbx.sync.config.has_valid_token")
@patch("dbx.sync.config.get_config")
def test_get_databricks_config_default_config_error(mock_get_config, mock_has_valid_token):
    mock_get_config.side_effect = databricks_cli.utils.InvalidConfigurationError()
    with pytest.raises(click.UsageError):
        get_databricks_config()
    assert mock_get_config.call_count == 1
    assert mock_has_valid_token.call_count == 0


@patch("dbx.sync.config.has_valid_token")
@patch("dbx.sync.config.ProfileConfigProvider")
def test_get_databricks_config_default(mock_provider_class, mock_has_valid_token):
    config = MagicMock()
    instance = mock_provider_class.return_value
    instance.get_config.return_value = config
    mock_has_valid_token.return_value = True
    assert get_databricks_config("foo") == config
    assert mock_has_valid_token.call_args == call(config)
    assert mock_provider_class.call_count == 1
    assert mock_provider_class.call_args == call("foo")
