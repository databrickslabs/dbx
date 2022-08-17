from unittest.mock import patch, Mock, MagicMock

import pytest
from databricks_cli.sdk import ApiClient, ClusterService

from dbx.api.cluster import ClusterController
from dbx.api.context import LocalContextManager
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
            [
                "execute",
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


def test_smoke_execute_workflow(
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
            [
                "execute",
                "--deployment-file=conf/deployment.yml",
                "--environment=default",
                "--cluster-id=000-some-cluster-id",
                f"{temp_project.name}-sample-etl-2.0",
            ],
        )

    assert execute_result.exit_code == 0


def test_smoke_execute_spark_python_task(
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
            [
                "execute",
                "--deployment-file",
                "conf/deployment.yml",
                "--environment",
                "default",
                "--cluster-id",
                "000-some-cluster-id",
                "--job",
                f"{temp_project.name}-sample-multitask",
                "--task",
                "etl",
            ],
        )

    assert execute_result.exit_code == 0


def test_smoke_execute_python_wheel_task(
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
            [
                "execute",
                "--deployment-file",
                "conf/deployment.yml",
                "--environment",
                "default",
                "--cluster-id",
                "000-some-cluster-id",
                "--job",
                f"{temp_project.name}-sample-multitask",
                "--task",
                "ml",
            ],
        )

    assert execute_result.exit_code == 0


def test_smoke_execute_python_wheel_task_with_params(
    temp_project,
    mock_api_v1_client,
    mock_api_v2_client,
    mock_local_context_manager,
    mlflow_file_uploader,
    mock_dbx_file_upload,
):  # noqa
    _params_options = ['{"parameters": ["a", 1]}', '{"named_parameters": ["--a=1", "--b=1"]}']
    mock_retval = {
        "status": "Finished",
        "results": {"resultType": "Ok", "data": "Ok!"},
    }
    for _params in _params_options:
        with patch(
            "dbx.api.client_provider.ApiV1Client.get_command_status",
            return_value=mock_retval,
        ):
            execute_result = invoke_cli_runner(
                [
                    "execute",
                    "--deployment-file",
                    "conf/deployment.yml",
                    "--environment",
                    "default",
                    "--cluster-id",
                    "000-some-cluster-id",
                    "--job",
                    f"{temp_project.name}-sample-multitask",
                    "--task",
                    "ml",
                    "--parameters",
                    _params,
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
    controller = ClusterController(api_client)
    with pytest.raises(RuntimeError):
        controller.preprocess_cluster_args(None, None)

    id_by_name = controller.preprocess_cluster_args("some-cluster-name", None)
    assert id_by_name == "aaa-111"

    id_by_id = controller.preprocess_cluster_args(None, "aaa-bbb-ccc")
    assert id_by_id == "aaa-bbb-ccc"

    negative_funcs = [
        lambda: controller.preprocess_cluster_args("non-existent-cluster-by-name", None),
        lambda: controller.preprocess_cluster_args("duplicated-name", None),
        lambda: controller.preprocess_cluster_args(None, "non-existent-id"),
    ]

    for func in negative_funcs:
        with pytest.raises(NameError):
            func()


def test_awake_cluster():
    # normal behavior
    client_mock = MagicMock()
    side_effect = [
        {"state": "TERMINATED"},
        {"state": "PENDING"},
        {"state": "RUNNING"},
        {"state": "RUNNING"},
    ]
    with patch.object(ClusterService, "get_cluster", side_effect=side_effect) as cluster_service_mock:
        controller = ClusterController(client_mock)
        controller.awake_cluster("aaa-bbb")
        assert cluster_service_mock("aaa-bbb").get("state") == "RUNNING"

    with patch.object(ClusterService, "get_cluster", return_value={"state": "ERROR"}):
        controller = ClusterController(client_mock)
        with pytest.raises(RuntimeError):
            controller.awake_cluster("aaa-bbb")
