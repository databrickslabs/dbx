import pytest
from pytest_mock import MockFixture

from databricks_cli.sdk import ApiClient
from dbx.api.auth import AuthConfigProvider
from dbx.api.client_provider import DatabricksClientProvider


@pytest.fixture(name="_cleanup_auth_cache")
def cleanup_auth_cache():
    method = AuthConfigProvider.get_config
    method.cache_clear()


@pytest.mark.usefixtures("_cleanup_auth_cache")
def test_get_v2_client_env_headers(mocker: MockFixture):
    azure_sp_token = "eyJhbAAAABBBB"
    workspace_id = (
        "/subscriptions/bc5bAAA-BBBB/resourceGroups/some-resource-group"
        "/providers/Microsoft.Databricks/workspaces/target-dtb-ws"
    )
    org_id = "1928374655647382"
    expected_headers = {
        "X-Databricks-Azure-SP-Management-Token": azure_sp_token,
        "X-Databricks-Azure-Workspace-Resource-Id": workspace_id,
        "X-Databricks-Org-Id": org_id,
    }
    host = "https://some-other-host"
    token = "dbapiAAABBB"

    expected_client = ApiClient(
        host=host,
        token=token,
        default_headers=expected_headers,
        command_name="cicdtemplates-",
    )
    mocker.patch.dict(
        "os.environ",
        {
            "DATABRICKS_HOST": host,
            "DATABRICKS_TOKEN": token,
            "AZURE_SERVICE_PRINCIPAL_TOKEN": azure_sp_token,
            "WORKSPACE_ID": workspace_id,
            "ORG_ID": org_id,
        },
        clear=True,
    )
    returned_client = DatabricksClientProvider.get_v2_client()
    assert returned_client.url == expected_client.url
    assert returned_client.default_headers == expected_client.default_headers


@pytest.mark.usefixtures("_cleanup_auth_cache")
def test_get_v2_client_arg_headers(mocker: MockFixture):
    expected_headers = {
        "X-Databricks-Azure-SP-Management-Token": "eyJhbAAAABBBB",
        "X-Databricks-Azure-Workspace-Resource-Id": (
            "/subscriptions/bc5bAAA-BBBB/resourceGroups/some-resource-group"
            "/providers/Microsoft.Databricks/workspaces/target-dtb-ws"
        ),
        "X-Databricks-Org-Id": "1928374655647382",
    }
    config = {
        "host": "https://some-other-host",
        "token": "dbapiAAABBB",
        "default_headers": expected_headers,
    }
    expected_client = ApiClient(
        host=config["host"],
        token=config["token"],
        default_headers=config["default_headers"],
        command_name="cicdtemplates-",
    )
    mocker.patch.dict(
        "os.environ",
        {"DATABRICKS_HOST": config["host"], "DATABRICKS_TOKEN": config["token"]},
        clear=True,
    )
    returned_client = DatabricksClientProvider.get_v2_client(headers=expected_headers)
    assert returned_client.url == expected_client.url
    assert returned_client.default_headers == expected_client.default_headers
