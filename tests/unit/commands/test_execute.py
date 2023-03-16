from pathlib import Path
from unittest.mock import patch, Mock, MagicMock

import pytest
from pytest_mock import MockerFixture
from databricks_cli.sdk import ApiClient, ClusterService

from dbx.api.cluster import ClusterController
from dbx.api.context import LocalContextManager
from dbx.commands.execute import parse_multiple
from dbx.models.files.context import ContextInfo
from tests.unit.conftest import invoke_cli_runner


@pytest.fixture()
def mock_local_context_manager(mocker):
    mocker.patch.object(LocalContextManager, "set_context", MagicMock())
    mocker.patch.object(
        LocalContextManager, "get_context", MagicMock(return_value=ContextInfo(context_id="some-context-id"))
    )


def test_smoke_execute_bad_argument(temp_project):
    execute_result = invoke_cli_runner(
        [
            "execute",
            "--cluster-id",
            "000-some-cluster-id",
            "--job",
            f"{temp_project.name}-sample-etl",
            "--parameters",
            "{some-bad-json]",
        ],
        expected_error=True,
    )
    assert "Provided parameters payload cannot be" in execute_result.stdout


def test_smoke_execute(
    temp_project,
    mock_api_v1_client,
    mock_api_v2_client,
    mock_local_context_manager,
    mlflow_file_uploader,
    mock_storage_io,
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
                f"{temp_project.name}-sample-etl",
                "--task=main",
            ],
        )

    assert execute_result.exit_code == 0


def test_smoke_execute_workflow(
    temp_project,
    mock_api_v1_client,
    mock_api_v2_client,
    mock_local_context_manager,
    mlflow_file_uploader,
    mock_storage_io,
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
                f"{temp_project.name}-sample-etl",
                "--task=main",
            ],
        )

    assert execute_result.exit_code == 0


def test_smoke_execute_spark_python_task(
    temp_project,
    mock_api_v1_client,
    mock_api_v2_client,
    mock_local_context_manager,
    mlflow_file_uploader,
    mock_storage_io,
    mocker,
):  # noqa
    mocker.patch(
        "dbx.api.client_provider.ApiV1Client.get_command_status",
        MagicMock(
            return_value={
                "status": "Finished",
                "results": {"resultType": "Ok", "data": "Ok!"},
            }
        ),
    )
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
    mock_storage_io,
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


@pytest.mark.parametrize("param_set", ['{"parameters": ["a", 1]}', '{"named_parameters": {"a":1}}'])
def test_smoke_execute_python_wheel_task_with_params(
    param_set,
    temp_project,
    mock_api_v1_client,
    mock_api_v2_client,
    mock_local_context_manager,
    mlflow_file_uploader,
    mock_storage_io,
):  # noqa
    mock_retval = {
        "status": "Finished",
        "results": {"resultType": "Ok", "data": "Ok!"},
    }
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
                param_set,
            ],
        )

    assert execute_result.exit_code == 0


def test_smoke_execute_spark_python_task_with_params(
    temp_project,
    mock_api_v1_client,
    mock_api_v2_client,
    mock_local_context_manager,
    mlflow_file_uploader,
    mock_storage_io,
):  # noqa
    mock_retval = {
        "status": "Finished",
        "results": {"resultType": "Ok", "data": "Ok!"},
    }
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
                "etl",
                "--parameters",
                '{"parameters": ["a", 1]}',
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
        ClusterController(api_client, cluster_name=None, cluster_id=None)

    c1 = ClusterController(api_client, "some-cluster-name", None)
    assert c1.cluster_id == "aaa-111"

    c2 = ClusterController(api_client, None, "aaa-bbb-ccc")
    assert c2.cluster_id == "aaa-bbb-ccc"

    negative_controllers = [
        lambda: ClusterController(api_client, "non-existent-cluster-by-name", None),
        lambda: ClusterController(api_client, "duplicated-name", None),
        lambda: ClusterController(api_client, None, "non-existent-id"),
    ]

    for _c in negative_controllers:
        with pytest.raises(NameError):
            _c()


@patch(
    "databricks_cli.clusters.api.ClusterService.get_cluster",
    side_effect=lambda cid: "something" if cid in ("aaa-bbb-ccc", "aaa-111") else None,
)
def test_awake_cluster(*_):
    # normal behavior
    client_mock = MagicMock()
    side_effect = [
        {"state": "TERMINATED"},
        {"state": "PENDING"},
        {"state": "RUNNING"},
        {"state": "RUNNING"},
    ]
    with patch.object(ClusterService, "get_cluster", side_effect=side_effect) as cluster_service_mock:
        controller = ClusterController(client_mock, None, "aaa-bbb-ccc")
        controller.awake_cluster()
        assert cluster_service_mock("aaa-bbb").get("state") == "RUNNING"

    with patch.object(ClusterService, "get_cluster", return_value={"state": "ERROR"}):
        controller = ClusterController(client_mock, None, "aaa-bbb-ccc")
        with pytest.raises(RuntimeError):
            controller.awake_cluster()


@pytest.mark.usefixtures(
    "mlflow_file_uploader",
    "mock_local_context_manager",
    "mock_storage_io",
    "mock_api_v1_client",
    "mock_api_v2_client",
)
def test_smoke_execute_additional_headers(mocker: MockerFixture, temp_project: Path):
    expected_headers = {
        "azure_sp_token": "eyJhbAAAABBBB",
        "workspace_id": (
            "/subscriptions/bc5bAAA-BBBB/resourceGroups/some-resource-group"
            "/providers/Microsoft.Databricks/workspaces/target-dtb-ws"
        ),
        "org_id": "1928374655647382",
    }
    env_mock = mocker.patch("dbx.commands.execute.prepare_environment", MagicMock())
    header_parse_mock = mocker.patch("dbx.commands.execute.parse_multiple", wraps=parse_multiple)
    kwargs = [f"{key}={val}" for key, val in expected_headers.items()]
    cli_kwargs = [arg for kw in kwargs for arg in ("--header", kw)]
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
                *cli_kwargs,
            ],
        )

    assert execute_result.exit_code == 0
    header_parse_mock.assert_called_once_with(kwargs)
    env_mock.assert_called_once_with("default", expected_headers)
