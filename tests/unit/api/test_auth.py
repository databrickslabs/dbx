from unittest.mock import MagicMock

import pytest
from pytest_mock import MockFixture

from dbx.api.auth import AuthConfigProvider, DbxConfig, DbxProfileConfigProvider, ProfileEnvConfigProvider


@pytest.fixture(name="_cleanup_auth_cache")
def cleanup_auth_cache():
    method = AuthConfigProvider.get_config
    method.cache_clear()


def test_auth_profile_non_existent(mocker: MockFixture, _cleanup_auth_cache):
    profile_name = "test-non-existent-profile"
    mocker.patch.dict("os.environ", {ProfileEnvConfigProvider.DBX_PROFILE_ENV: profile_name}, clear=True)
    with pytest.raises(Exception):
        AuthConfigProvider.get_config()


def test_auth_profile_positive(mocker: MockFixture, _cleanup_auth_cache):
    profile_name = "test-existent-profile"
    mocker.patch.dict("os.environ", {ProfileEnvConfigProvider.DBX_PROFILE_ENV: profile_name}, clear=True)
    expected_config = DbxConfig(host="https://some-host", token="dbapiAAABBB", username=None, password=None)
    mocker.patch.object(
        DbxProfileConfigProvider,
        "get_config",
        MagicMock(return_value=expected_config),
    )
    returned_config = AuthConfigProvider.get_config()
    assert expected_config.host == returned_config.host
    assert expected_config.token == returned_config.token
    assert returned_config.headers == {}


def test_auth_env_positive(mocker: MockFixture, _cleanup_auth_cache):
    expected_config = DbxConfig(host="https://some-other-host", token="dbapiAAABBB", username=None, password=None)
    mocker.patch.dict(
        "os.environ", {"DATABRICKS_HOST": expected_config.host, "DATABRICKS_TOKEN": expected_config.token}, clear=True
    )
    returned_config = AuthConfigProvider.get_config()
    assert expected_config.host == returned_config.host
    assert expected_config.token == returned_config.token
    assert returned_config.headers == {}


def test_auth_both(mocker: MockFixture, _cleanup_auth_cache):
    expected_env_config = DbxConfig(
        host="https://some-env-based-host", token="dbapiAAABBB", username=None, password=None
    )
    profile_name = "test-existent-profile"
    mocker.patch.object(
        DbxProfileConfigProvider,
        "get_config",
        MagicMock(
            return_value=DbxConfig(host="https://some-profile-host", token="dbapiAAABBB", username=None, password=None)
        ),
    )
    mocker.patch.dict(
        "os.environ",
        {
            "DATABRICKS_HOST": expected_env_config.host,
            "DATABRICKS_TOKEN": expected_env_config.token,
            ProfileEnvConfigProvider.DBX_PROFILE_ENV: profile_name,
        },
        clear=True,
    )
    returned_config = AuthConfigProvider.get_config()
    assert expected_env_config.host == returned_config.host
    assert expected_env_config.token == returned_config.token
    assert returned_config.headers == {}


def test_auth_none(_cleanup_auth_cache):
    with pytest.raises(Exception):
        AuthConfigProvider.get_config()


@pytest.mark.usefixtures("_cleanup_auth_cache")
def test_auth_env_aad_positive(mocker: MockFixture):
    expected_headers = {
        "azure_sp_token": "eyJhbAAAABBBB",
        "workspace_id": (
            "/subscriptions/bc5bAAA-BBBB/resourceGroups/some-resource-group"
            "/providers/Microsoft.Databricks/workspaces/target-dtb-ws"
        ),
        "org_id": "1928374655647382",
    }
    expected_config = DbxConfig(
        host="https://some-other-host",
        username=None,
        password=None,
        token="dbapiAAABBB",
        azure_sp_token=expected_headers["azure_sp_token"],
        workspace_id=expected_headers["workspace_id"],
        org_id=expected_headers["org_id"],
    )
    mocker.patch.dict(
        "os.environ",
        {
            "DATABRICKS_HOST": expected_config.host,
            "DATABRICKS_TOKEN": expected_config.token,
            "AZURE_SERVICE_PRINCIPAL_TOKEN": expected_headers["azure_sp_token"],
            "WORKSPACE_ID": expected_headers["workspace_id"],
            "ORG_ID": expected_headers["org_id"],
        },
        clear=True,
    )
    returned_config = AuthConfigProvider.get_config()
    assert expected_config.host == returned_config.host
    assert expected_config.token == returned_config.token
    assert expected_config.headers == returned_config.headers


@pytest.mark.usefixtures("_cleanup_auth_cache")
def test_auth_profile_aad_positive(mocker: MockFixture):
    profile_name = "test-existent-profile"
    expected_config = DbxConfig(
        host="https://some-profile-host",
        username=None,
        password=None,
        token="dbapiAAABBB",
        azure_sp_token="eyJhbAAAABBBB",
        workspace_id=(
            "/subscriptions/bc5bAAA-BBBB/resourceGroups/some-resource-group"
            "/providers/Microsoft.Databricks/workspaces/target-dtb-ws"
        ),
        org_id="1928374655647382",
    )
    mocker.patch.dict("os.environ", {ProfileEnvConfigProvider.DBX_PROFILE_ENV: profile_name}, clear=True)
    mocker.patch.object(
        DbxProfileConfigProvider,
        "get_config",
        MagicMock(return_value=expected_config),
    )

    returned_config = AuthConfigProvider.get_config()
    assert expected_config.host == returned_config.host
    assert expected_config.token == returned_config.token
    assert expected_config.headers == returned_config.headers


def test_config_validity():
    good_config = DbxConfig(host="https://some-host", token="dbapiAAABBB", username=None, password=None)
    bad_config = DbxConfig(host="https://some-host", username="some-username", password="some-password", token=None)
    AuthConfigProvider._verify_config_validity(good_config)

    with pytest.raises(Exception):
        AuthConfigProvider._verify_config_validity(bad_config)


@pytest.mark.usefixtures("_cleanup_auth_cache")
def test_resolution_order(mocker: MockFixture):
    env_config = DbxConfig(host="https://some-env-based-host", token="dbapiAAABBB", username=None, password=None)
    mocker.patch.dict(
        "os.environ",
        {
            "DATABRICKS_HOST": env_config.host,
            "DATABRICKS_TOKEN": env_config.token,
        },
        clear=True,
    )
    AuthConfigProvider.get_config()
    assert env_config.host == env_config.host
    assert env_config.token == env_config.token
