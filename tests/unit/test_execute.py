from unittest.mock import patch, Mock, MagicMock

import pytest
from databricks_cli.sdk import ApiClient, ClusterService

from dbx.api.context import LocalContextManager
from dbx.commands.execute import execute, awake_cluster  # noqa
from dbx.utils.common import _preprocess_cluster_args
from tests.unit.conftest import invoke_cli_runner


@pytest.fixture()
def mock_local_context_manager(mocker):
    mocker.patch.object(LocalContextManager, "set_context", MagicMock())
    mocker.patch.object(LocalContextManager, "get_context", MagicMock(return_value="some-context-id"))


def test_smoke_execute(
    temp_project,
    mock_api_v1_client,
    mock_api_v2_client,
    mock_local_context_manager,
    mlflow_file_uploader,
    mock_dbx_file_upload,
):  # noqa
    with patch(
        "dbx.api.client_provider.ApiV1Client.get_command_status",
        return_value={
            "status": "Finished",
            "results": {"resultType": "Ok", "data": "Ok!"},
        },
    ):
        execute_result = invoke_cli_runner(
            execute,
            [
                "--deployment-file",
                "conf/deployment.yml",
                "--environment",
                "default",
                "--cluster-id",
                "000-some-cluster-id",
                "--job",
                f"{temp_project.name}-sample-etl-2.0",
            ],
        )

    assert execute_result.exit_code == 0


@patch(
    "databricks_cli.clusters.api.ClusterService.list_clusters",
    return_value={
        "clusters": [
            {"cluster_name": "some-cluster-name", "cluster_id": "aaa-111"},
            {"cluster_name": "other-cluster-name", "cluster_id": "aaa-bbb-ccc"},
            {"cluster_name": "duplicated-name", "cluster_id": "duplicated-1"},
            {"cluster_name": "duplicated-name", "cluster_id": "duplicated-2"},
        ]
    },
)
@patch(
    "databricks_cli.clusters.api.ClusterService.get_cluster",
    side_effect=lambda cid: "something" if cid in ("aaa-bbb-ccc", "aaa-111") else None,
)
def test_preprocess_cluster_args(*_):  # noqa
    api_client = Mock(ApiClient)

    with pytest.raises(RuntimeError):
        _preprocess_cluster_args(api_client, None, None)

    id_by_name = _preprocess_cluster_args(api_client, "some-cluster-name", None)
    assert id_by_name == "aaa-111"

    id_by_id = _preprocess_cluster_args(api_client, None, "aaa-bbb-ccc")
    assert id_by_id == "aaa-bbb-ccc"

    negative_funcs = [
        lambda: _preprocess_cluster_args(api_client, "non-existent-cluster-by-name", None),
        lambda: _preprocess_cluster_args(api_client, "duplicated-name", None),
        lambda: _preprocess_cluster_args(api_client, None, "non-existent-id"),
    ]

    for func in negative_funcs:
        with pytest.raises(NameError):
            func()


def test_awake_cluster():
    # normal behavior
    cluster_service_mock = Mock(ClusterService)
    cluster_service_mock.get_cluster.side_effect = [
        {"state": "TERMINATED"},
        {"state": "PENDING"},
        {"state": "RUNNING"},
        {"state": "RUNNING"},
    ]
    awake_cluster(cluster_service_mock, "aaa-bbb")
    assert cluster_service_mock.get_cluster("aaa-bbb").get("state") == "RUNNING"

    # error behavior
    error_mock = Mock(ClusterService)
    error_mock.get_cluster.return_value = {"state": "ERROR"}
    with pytest.raises(RuntimeError):
        awake_cluster(error_mock, "aaa-bbb")
