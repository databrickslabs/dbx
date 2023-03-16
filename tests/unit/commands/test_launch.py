import textwrap
from pathlib import Path
from typing import List, Optional
from unittest.mock import MagicMock, PropertyMock

import pytest
from databricks_cli.sdk import JobsService
from pytest_mock import MockFixture

from dbx.api.client_provider import DatabricksClientProvider
from dbx.api.config_reader import ConfigReader
from dbx.api.launch.pipeline_models import PipelineUpdateState
from dbx.api.launch.runners.base import PipelineUpdateResponse
from dbx.api.launch.runners.pipeline import PipelineLauncher
from dbx.api.launch.tracer import RunTracer, PipelineTracer
from dbx.api.services.jobs import JobListing, ListJobsResponse
from dbx.api.services.pipelines import NamedPipelinesService
from dbx.api.storage.io import StorageIO
from dbx.utils.common import parse_multiple
from dbx.utils.json import JsonUtils
from tests.unit.conftest import invoke_cli_runner


def deploy_and_get_job_name(deploy_args: Optional[List[str]] = None) -> str:
    if deploy_args is None:
        deploy_args = []

    deploy_result = invoke_cli_runner(["deploy"] + deploy_args)
    assert deploy_result.exit_code == 0
    deployment_info = ConfigReader(Path("conf/deployment.yml")).get_environment("default")
    _chosen_job = deployment_info.payload.workflows[0].name
    return _chosen_job


def prepare_job_service_mock(mocker: MockFixture, job_name):
    jobs_payload = {
        "jobs": [
            {
                "settings": {
                    "name": job_name,
                },
                "job_id": 1,
            }
        ]
    }
    response = ListJobsResponse(**jobs_payload)
    mocker.patch.object(JobListing, "by_name", MagicMock(return_value=response))


def prepare_tracing_mock(mocker: MockFixture, final_result_state: str):
    mocker.patch.object(
        JobsService,
        "get_run",
        MagicMock(
            side_effect=[
                {
                    "run_id": "1",
                    "run_page_url": "http://some",
                    "state": {"state_message": "RUNNING", "result_state": None},
                },
                {
                    "run_id": "1",
                    "run_page_url": "http://some",
                    "state": {
                        "state_message": "RUNNING",
                        "life_cycle_state": "TERMINATED",
                        "result_state": final_result_state,
                    },
                },
            ]
        ),
    )


def test_smoke_launch(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name()
    prepare_job_service_mock(mocker, _chosen_job)

    launch_job_result = invoke_cli_runner(["launch", "--job", _chosen_job])
    assert launch_job_result.exit_code == 0

    launch_submit_result = invoke_cli_runner(["launch", "--job", _chosen_job, "--as-run-submit"], expected_error=True)

    assert launch_submit_result.exception is not None


def test_smoke_launch_workflow(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name()
    prepare_job_service_mock(mocker, _chosen_job)

    launch_job_result = invoke_cli_runner(["launch", _chosen_job])
    assert launch_job_result.exit_code == 0


def test_launch_no_arguments(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name()
    prepare_job_service_mock(mocker, _chosen_job)

    launch_job_result = invoke_cli_runner(["launch"], expected_error=True)
    assert "Please provide workflow name as an argument" in str(launch_job_result.exception)


def test_parametrized_tags(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client
):
    tags_definition = ["--tags", "cake=cheesecake", "--branch-name", "test-branch"]
    _chosen_job = deploy_and_get_job_name(tags_definition)
    prepare_job_service_mock(mocker, _chosen_job)

    launch_result = invoke_cli_runner(["launch", "--job", _chosen_job] + tags_definition)
    assert launch_result.exit_code == 0


def test_long_tags_list(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client
):
    tags_definition = [
        "--tags",
        "cake=cheesecake",
        "--branch-name",
        "test-branch",
        "--tags",
        "mock=pock",
        "--tags",
        "soup=beautiful",
    ]
    _chosen_job = deploy_and_get_job_name(tags_definition)
    prepare_job_service_mock(mocker, _chosen_job)

    launch_result = invoke_cli_runner(["launch", "--job", _chosen_job] + tags_definition)
    assert launch_result.exit_code == 0


def test_unmatched_deploy_and_launch(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name()
    prepare_job_service_mock(mocker, _chosen_job)

    launch_result = invoke_cli_runner(["launch", "--job", _chosen_job] + ["--as-run-submit"], expected_error=True)
    assert launch_result.exception is not None


def test_launch_run_submit(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client
):
    deployment_result = Path(".dbx/deployment-result.json")
    _chosen_job = deploy_and_get_job_name(["--files-only", "--write-specs-to-file", deployment_result])
    mocked_result = JsonUtils.read(deployment_result)
    mocker.patch.object(StorageIO, "load", MagicMock(return_value=mocked_result))
    launch_result = invoke_cli_runner(["launch", "--job", _chosen_job] + ["--as-run-submit"])
    assert launch_result.exit_code == 0


def test_multi_launch(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name()
    prepare_job_service_mock(mocker, _chosen_job)

    l1_result = invoke_cli_runner(["launch", "--job", _chosen_job])
    l2_result = invoke_cli_runner(["launch", "--job", _chosen_job])
    assert l1_result.exception is None
    assert l2_result.exception is None


def test_launch_empty_runs(temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client):
    _chosen_job = deploy_and_get_job_name(["--files-only", "--tags", "cake=strudel"])
    launch_result = invoke_cli_runner(
        ["launch", "--job", _chosen_job] + ["--as-run-submit", "--tags", "cake=cheesecake"], expected_error=True
    )
    assert "No deployments provided per given set of filters" in str(launch_result.exception)


def test_launch_with_output(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name()
    prepare_job_service_mock(mocker, _chosen_job)
    launch_result = invoke_cli_runner(["launch", "--job", _chosen_job] + ["--include-output=stdout"])
    assert launch_result.exit_code == 0


def test_launch_with_unparsable_params(temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client):
    _chosen_job = deploy_and_get_job_name()
    launch_result = invoke_cli_runner(
        ["launch", "--job", _chosen_job, "--parameters", "{very[bad]_json}"], expected_error=True
    )
    assert "Provided parameters payload cannot be" in launch_result.stdout


def test_launch_with_run_now_v21_params(mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io):
    client_mock = MagicMock()
    p = PropertyMock(return_value="2.1")
    type(client_mock).jobs_api_version = p
    mocker.patch.object(DatabricksClientProvider, "get_v2_client", lambda x: client_mock)
    _chosen_job = deploy_and_get_job_name()
    prepare_job_service_mock(mocker, _chosen_job)
    launch_result = invoke_cli_runner(
        ["launch", "--job", _chosen_job, "--parameters", '{"python_named_params":{"a":1}}']
    )
    assert launch_result.exit_code == 0


def test_launch_with_run_now_v20_params(mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io):
    client_mock = MagicMock()
    type(client_mock).jobs_api_version = PropertyMock(return_value="2.0")
    mocker.patch.object(DatabricksClientProvider, "get_v2_client", lambda x: client_mock)
    _chosen_job = deploy_and_get_job_name()
    prepare_job_service_mock(mocker, _chosen_job)
    launch_result = invoke_cli_runner(["launch", "--job", _chosen_job, "--parameters", '{"python_params":[1,2]}'])
    assert launch_result.exit_code == 0


def test_launch_with_trace(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name(["--tags", "soup=beautiful"])
    prepare_job_service_mock(mocker, _chosen_job)
    prepare_tracing_mock(mocker, "SUCCESS")
    launch_result = invoke_cli_runner(["launch", "--job", _chosen_job] + ["--tags", "soup=beautiful", "--trace"])
    assert launch_result.exit_code == 0


@pytest.mark.parametrize("state, err", [(PipelineUpdateState.COMPLETED, None), (PipelineUpdateState.FAILED, Exception)])
def test_launch_pipeline(
    state, err, mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client
):
    (temp_project / "conf" / "deployment.yml").write_text(
        textwrap.dedent(
            """
    environments:
      default:
        workflows:
          - name: "some"
            workflow_type: "pipeline"
            target: "some"
            libraries:
              - notebook:
                  path: "/Repos/some/path"
    """
        )
    )
    mocker.patch.object(NamedPipelinesService, "find_by_name_strict", MagicMock(return_value=1))
    mocker.patch.object(
        PipelineLauncher, "launch", MagicMock(return_value=(PipelineUpdateResponse(update_id="a", request_id="a"), 1))
    )
    invoke_cli_runner(["deploy", "some"])

    mocker.patch.object(PipelineTracer, "start", MagicMock(return_value=state))

    if err:
        launch_result = invoke_cli_runner(["launch", "some", "-p", "--trace"], expected_error=True)
        assert "failed during execution" in str(launch_result.exception)
    else:
        launch_result = invoke_cli_runner(["launch", "some", "-p", "--trace"])
        assert launch_result.exit_code == 0


def test_launch_with_trace_failed(
    mock_storage_io, mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name(["--tags", "soup=beautiful"])
    prepare_job_service_mock(mocker, _chosen_job)
    prepare_tracing_mock(mocker, "ERROR")
    launch_result = invoke_cli_runner(
        ["launch", "--job", _chosen_job] + ["--tags", "soup=beautiful", "--trace"], expected_error=True
    )
    assert "Tracked run failed during execution" in str(launch_result.exception)


def test_launch_with_trace_and_kill_on_sigterm(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name(["--tags", "soup=beautiful"])
    prepare_job_service_mock(mocker, _chosen_job)
    prepare_tracing_mock(mocker, "SUCCESS")
    launch_result = invoke_cli_runner(
        ["launch", "--job", _chosen_job] + ["--tags", "soup=beautiful", "--trace", "--kill-on-sigterm"]
    )
    assert launch_result.exit_code == 0


def test_launch_with_trace_and_kill_on_sigterm_with_interruption(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name(["--tags", "soup=beautiful"])
    prepare_job_service_mock(mocker, _chosen_job)
    _tracer = mocker.patch.object(RunTracer, "start", return_value=("SUCCESS", {}))
    launch_result = invoke_cli_runner(
        ["launch", "--job", _chosen_job] + ["--tags", "soup=beautiful", "--trace", "--kill-on-sigterm"]
    )
    assert launch_result.exit_code == 0
    _tracer.assert_called_once()


def test_smoke_launch_workflow_additional_headers(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name()
    prepare_job_service_mock(mocker, _chosen_job)
    expected_headers = {
        "azure_sp_token": "eyJhbAAAABBBB",
        "workspace_id": (
            "/subscriptions/bc5bAAA-BBBB/resourceGroups/some-resource-group"
            "/providers/Microsoft.Databricks/workspaces/target-dtb-ws"
        ),
        "org_id": "1928374655647382",
    }
    header_parse_mock = mocker.patch("dbx.commands.launch.parse_multiple", wraps=parse_multiple)
    kwargs = [f"{key}={val}" for key, val in expected_headers.items()]
    cli_kwargs = [arg for kw in kwargs for arg in ("--header", kw)]

    launch_job_result = invoke_cli_runner(["launch", _chosen_job, *cli_kwargs])
    assert launch_job_result.exit_code == 0
    header_parse_mock.assert_any_call(kwargs)
