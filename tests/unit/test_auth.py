from unittest.mock import MagicMock

import pytest
from databricks_cli.configure.provider import ProfileConfigProvider, DatabricksConfig
from pytest_mock import MockFixture

from dbx.api.auth import AuthConfigProvider


def test_auth_profile_non_existent(mocker: MockFixture):
    profile_name = "test-non-existent-profile"
    mocker.patch.dict("os.environ", {AuthConfigProvider.DBX_PROFILE_ENV: profile_name}, clear=True)
    with pytest.raises(Exception):
        AuthConfigProvider.get_config()


def test_auth_profile_positive(mocker: MockFixture):
    profile_name = "test-existent-profile"
    mocker.patch.dict("os.environ", {AuthConfigProvider.DBX_PROFILE_ENV: profile_name}, clear=True)
    expected_config = DatabricksConfig(host="https://some-host", token="dbapiAAABBB", username=None, password=None)
    mocker.patch.object(
        ProfileConfigProvider,
        "get_config",
        MagicMock(return_value=expected_config),
    )
    returned_config = AuthConfigProvider.get_config()
    assert expected_config.host == returned_config.host
    assert expected_config.token == returned_config.token


def test_auth_env_positive(mocker: MockFixture):
    expected_config = DatabricksConfig(
        host="https://some-other-host", token="dbapiAAABBB", username=None, password=None
    )
    mocker.patch.dict(
        "os.environ", {"DATABRICKS_HOST": expected_config.host, "DATABRICKS_TOKEN": expected_config.token}, clear=True
    )
    returned_config = AuthConfigProvider.get_config()
    assert expected_config.host == returned_config.host
    assert expected_config.token == returned_config.token


def test_auth_both(mocker: MockFixture):
    expected_env_config = DatabricksConfig(
        host="https://some-env-based-host", token="dbapiAAABBB", username=None, password=None
    )
    profile_name = "test-existent-profile"
    mocker.patch.object(
        ProfileConfigProvider,
        "get_config",
        MagicMock(
            return_value=DatabricksConfig(
                host="https://some-profile-host", token="dbapiAAABBB", username=None, password=None
            )
        ),
    )
    mocker.patch.dict(
        "os.environ",
        {
            "DATABRICKS_HOST": expected_env_config.host,
            "DATABRICKS_TOKEN": expected_env_config.token,
            AuthConfigProvider.DBX_PROFILE_ENV: profile_name,
        },
        clear=True,
    )
    returned_config = AuthConfigProvider.get_config()
    assert expected_env_config.host == returned_config.host
    assert expected_env_config.token == returned_config.token


def test_auth_none():
    with pytest.raises(Exception):
        AuthConfigProvider.get_config()


def test_config_validity():
    good_config = DatabricksConfig(host="https://some-host", token="dbapiAAABBB", username=None, password=None)
    bad_config = DatabricksConfig(
        host="https://some-host", username="some-username", password="some-password", token=None
    )
    AuthConfigProvider._verify_config_validity(good_config)

    with pytest.raises(Exception):
        AuthConfigProvider._verify_config_validity(bad_config)
