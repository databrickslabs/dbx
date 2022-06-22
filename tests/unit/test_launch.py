from pathlib import Path
from typing import List, Optional
from unittest.mock import MagicMock

import pytest
from databricks_cli.sdk import JobsService
from pytest_mock import MockFixture

from dbx.api.config_reader import ConfigReader
from dbx.commands.deploy import deploy
from dbx.commands.launch import launch, _load_dbx_file, _define_payload_key, _trace_run, _cancel_run
from dbx.utils.json import JsonUtils
from .conftest import invoke_cli_runner, extract_function_name


def deploy_and_get_job_name(deploy_args: Optional[List[str]] = None) -> str:
    if deploy_args is None:
        deploy_args = []

    deploy_result = invoke_cli_runner(deploy, deploy_args)
    assert deploy_result.exit_code == 0
    _chosen_job = ConfigReader().get_environment("default")["jobs"][0]["name"]
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
    mocker.patch.object(JobsService, "list_jobs", MagicMock(return_value=jobs_payload))


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
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name()
    prepare_job_service_mock(mocker, _chosen_job)

    launch_job_result = invoke_cli_runner(launch, ["--job", _chosen_job])
    assert launch_job_result.exit_code == 0

    launch_submit_result = invoke_cli_runner(launch, ["--job", _chosen_job, "--as-run-submit"], expected_error=True)

    assert launch_submit_result.exception is not None


def test_parametrized_tags(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client
):
    tags_definition = ["--tags", "cake=cheesecake", "--branch-name", "test-branch"]
    _chosen_job = deploy_and_get_job_name(tags_definition)
    prepare_job_service_mock(mocker, _chosen_job)

    launch_result = invoke_cli_runner(launch, ["--job", _chosen_job] + tags_definition)
    assert launch_result.exit_code == 0


def test_long_tags_list(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client
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

    launch_result = invoke_cli_runner(launch, ["--job", _chosen_job] + tags_definition)
    assert launch_result.exit_code == 0


def test_unmatched_deploy_and_launch(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name()
    prepare_job_service_mock(mocker, _chosen_job)

    launch_result = invoke_cli_runner(launch, ["--job", _chosen_job] + ["--as-run-submit"], expected_error=True)
    assert launch_result.exception is not None


def test_launch_run_submit(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client
):
    deployment_result = Path(".dbx/deployment-result.json")
    _chosen_job = deploy_and_get_job_name(["--files-only", "--write-specs-to-file", deployment_result])
    mocked_result = JsonUtils.read(deployment_result)
    mocker.patch(extract_function_name(_load_dbx_file), MagicMock(return_value=mocked_result))
    launch_result = invoke_cli_runner(launch, ["--job", _chosen_job] + ["--as-run-submit"])
    assert launch_result.exit_code == 0


def test_launch_not_found(temp_project: Path, mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client):
    _chosen_job = deploy_and_get_job_name(["--tags", "soup=beautiful"])
    launch_result = invoke_cli_runner(
        launch, ["--job", _chosen_job] + ["--tags", "cake=cheesecake"], expected_error=True
    )
    assert "please verify tag existence in the UI" in str(launch_result.exception)


def test_launch_empty_runs(temp_project: Path, mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client):
    _chosen_job = deploy_and_get_job_name(["--files-only", "--tags", "cake=strudel"])
    launch_result = invoke_cli_runner(
        launch, ["--job", _chosen_job] + ["--as-run-submit", "--tags", "cake=cheesecake"], expected_error=True
    )
    assert "No deployments provided per given set of filters" in str(launch_result.exception)


def test_launch_with_trace(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name(["--tags", "soup=beautiful"])
    prepare_job_service_mock(mocker, _chosen_job)
    prepare_tracing_mock(mocker, "SUCCESS")
    launch_result = invoke_cli_runner(launch, ["--job", _chosen_job] + ["--tags", "soup=beautiful", "--trace"])
    assert launch_result.exit_code == 0


def test_launch_with_trace_failed(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name(["--tags", "soup=beautiful"])
    prepare_job_service_mock(mocker, _chosen_job)
    prepare_tracing_mock(mocker, "ERROR")
    launch_result = invoke_cli_runner(
        launch, ["--job", _chosen_job] + ["--tags", "soup=beautiful", "--trace"], expected_error=True
    )
    assert "Tracked run failed during execution" in str(launch_result.exception)


def test_launch_with_trace_and_kill_on_sigterm(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name(["--tags", "soup=beautiful"])
    prepare_job_service_mock(mocker, _chosen_job)
    prepare_tracing_mock(mocker, "SUCCESS")
    launch_result = invoke_cli_runner(
        launch, ["--job", _chosen_job] + ["--tags", "soup=beautiful", "--trace", "--kill-on-sigterm"]
    )
    assert launch_result.exit_code == 0


def test_launch_with_trace_and_kill_on_sigterm_with_interruption(
    mocker: MockFixture, temp_project: Path, mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client
):
    _chosen_job = deploy_and_get_job_name(["--tags", "soup=beautiful"])
    prepare_job_service_mock(mocker, _chosen_job)
    mocker.patch(extract_function_name(_trace_run), MagicMock(side_effect=[KeyboardInterrupt("stopped!")])),
    cancel_run_mock = mocker.patch(extract_function_name(_cancel_run))
    launch_result = invoke_cli_runner(
        launch, ["--job", _chosen_job] + ["--tags", "soup=beautiful", "--trace", "--kill-on-sigterm"]
    )
    assert launch_result.exit_code == 0
    cancel_run_mock.assert_called_once()


# @patch(
#     "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
#     return_value=test_dbx_config,
# )
# @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
# @patch("mlflow.set_experiment", return_value=None)
# @patch("mlflow.log_artifact", return_value=None)
# @patch("mlflow.set_tags", return_value=None)
# @patch("mlflow.search_runs", return_value=pd.DataFrame([{"run_id": 1, "tags.cake": "cheesecake"}]))
# @patch("databricks_cli.dbfs.api.DbfsService.read", return_value=DEFAULT_DATA_MOCK)
# @patch("databricks_cli.jobs.api.JobsService.list_runs", return_value={"runs": []})
# @patch(
#     "databricks_cli.jobs.api.JobsService.list_jobs",
#     return_value={
#         "jobs": [
#             {
#                 "settings": {
#                     "name": "sample",
#                     "spark_python_task": {
#                         "python_file": "some.entrypoint.py",
#                     },
#                 },
#                 "job_id": 1,
#             }
#         ]
#     },
# )
# @patch("databricks_cli.jobs.api.JobsService.run_now", return_value={"run_id": "1"})
# @patch("databricks_cli.jobs.api.JobsService.get_run", return_value={"run_id": "1", "run_page_url": "http://some"})
# def test_launch_with_parameters(self, *_):
#     with self.project_dir:
#         ws_dir = "/Shared/dbx/projects/%s" % self.project_name
#         configure_result = invoke_cli_runner(
#             configure,
#             [
#                 "--environment",
#                 "test",
#                 "--profile",
#                 self.profile_name,
#                 "--workspace-dir",
#                 ws_dir,
#             ],
#         )
#         self.assertEqual(configure_result.exit_code, 0)
#
#         deployment_content = {"test": {"dbfs": {}, "jobs": []}}
#
#         JsonUtils.write(DEFAULT_DEPLOYMENT_FILE_PATH, deployment_content)
#
#         def _test():
#             deploy_result = invoke_cli_runner(deploy, ["--environment", "test", "--tags", "cake=cheesecake"])
#
#             self.assertEqual(deploy_result.exit_code, 0)
#
#             launch_raw = invoke_cli_runner(
#                 launch,
#                 [
#                     "--environment",
#                     "test",
#                     "--job",
#                     "sample",
#                     "--tags",
#                     "cake=cheesecake",
#                     """--parameters-raw='{"key1": "value1", "key2": 2}'""",
#                 ],
#             )
#
#             self.assertEqual(launch_raw.exit_code, 0)
#
#             launch_with_params = invoke_cli_runner(
#                 launch,
#                 [
#                     "--environment",
#                     "test",
#                     "--job",
#                     "sample",
#                     "--tags",
#                     "cake=cheesecake",
#                     "--parameters='cake=cheesecake'",
#                 ],
#             )
#
#             self.assertEqual(launch_with_params.exit_code, 0)
#
#         self._launch_in_provided_context(_test)
#
# @patch(
#     "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
#     return_value=test_dbx_config,
# )
# @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
# @patch("mlflow.set_experiment", return_value=None)
# @patch("mlflow.log_artifact", return_value=None)
# @patch("mlflow.set_tags", return_value=None)
# @patch(
#     "mlflow.search_runs",
#     return_value=pd.DataFrame([{"run_id": 1, "tags.cake": "strudel", "tags.dbx_deploy_type": "files_only"}]),
# )
# @patch("databricks_cli.dbfs.api.DbfsService.read", return_value=RUN_SUBMIT_DATA_MOCK)
# @patch("databricks_cli.jobs.api.JobsService.list_runs", return_value={"runs": []})
# @patch("dbx.commands.launch._submit_run", return_value={"run_id": 1})
# @patch("databricks_cli.jobs.api.JobsService.get_run", return_value={"run_id": "1", "run_page_url": "http://some"})
# def test_no_runs_run_submit(self, *_):
#     with self.project_dir:
#         ws_dir = "/Shared/dbx/projects/%s" % self.project_name
#         configure_result = invoke_cli_runner(
#             configure,
#             [
#                 "--environment",
#                 "test",
#                 "--profile",
#                 self.profile_name,
#                 "--workspace-dir",
#                 ws_dir,
#             ],
#         )
#         self.assertEqual(configure_result.exit_code, 0)
#
#         deployment_content = {"test": {"dbfs": {}, "jobs": []}}
#
#         JsonUtils.write(DEFAULT_DEPLOYMENT_FILE_PATH, deployment_content)
#
#         def _test():
#             deploy_result = invoke_cli_runner(deploy, ["--environment", "test", "--tags", "cake=cheesecake"])
#
#             self.assertEqual(deploy_result.exit_code, 0)
#
#             launch_result = invoke_cli_runner(
#                 launch,
#                 ["--environment", "test", "--job", "sample", "--tags", "cake=cheesecake", "--as-run-submit"],
#                 expected_error=True,
#             )
#
#             self.assertIsNotNone(launch_result.exception)
#             self.assertTrue("No deployments provided per given set of filters:" in str(launch_result.exception))
#
#         self._launch_in_provided_context(_test)
#
# @patch(
#     "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
#     return_value=test_dbx_config,
# )
# @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
# @patch("mlflow.set_experiment", return_value=None)
# @patch("mlflow.log_artifact", return_value=None)
# @patch("mlflow.set_tags", return_value=None)
# @patch("mlflow.search_runs", return_value=pd.DataFrame([]))
# @patch("databricks_cli.dbfs.api.DbfsService.read", return_value=DEFAULT_DATA_MOCK)
# @patch("databricks_cli.jobs.api.JobsService.list_runs", return_value={"runs": []})
# @patch("databricks_cli.jobs.api.JobsService.run_now", return_value={"run_id": "1"})
# def test_no_runs(self, *_):
#     with self.project_dir:
#         ws_dir = "/Shared/dbx/projects/%s" % self.project_name
#         configure_result = invoke_cli_runner(
#             configure,
#             [
#                 "--environment",
#                 "test",
#                 "--profile",
#                 self.profile_name,
#                 "--workspace-dir",
#                 ws_dir,
#             ],
#         )
#         self.assertEqual(configure_result.exit_code, 0)
#
#         deployment_content = {"test": {"dbfs": {}, "jobs": []}}
#
#         JsonUtils.write(DEFAULT_DEPLOYMENT_FILE_PATH, deployment_content)
#
#         def _test():
#             deploy_result = invoke_cli_runner(deploy, ["--environment", "test", "--tags", "cake=cheesecake"])
#
#             self.assertEqual(deploy_result.exit_code, 0)
#
#             launch_result = invoke_cli_runner(
#                 launch,
#                 [
#                     "--environment",
#                     "test",
#                     "--job",
#                     "sample",
#                     "--tags",
#                     "cake=cheesecake",
#                 ],
#                 expected_error=True,
#             )
#
#             self.assertIsNotNone(launch_result.exception)
#             self.assertTrue("not found in underlying MLflow experiment" in str(launch_result.exception))
#
#         self._launch_in_provided_context(_test)
#
# @patch(
#     "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
#     return_value=test_dbx_config,
# )
# @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
# @patch("mlflow.set_experiment", return_value=None)
# @patch("mlflow.log_artifact", return_value=None)
# @patch("mlflow.set_tags", return_value=None)
# @patch("mlflow.search_runs", return_value=pd.DataFrame([{"run_id": 1, "tags.cake": "cheesecake"}]))
# @patch("databricks_cli.dbfs.api.DbfsService.read", return_value=DEFAULT_DATA_MOCK)
# @patch("databricks_cli.jobs.api.JobsService.list_runs", return_value={"runs": []})
# @patch(
#     "databricks_cli.jobs.api.JobsService.list_jobs",
#     return_value={
#         "jobs": [
#             {
#                 "settings": {
#                     "name": "sample",
#                 },
#                 "job_id": 1,
#             }
#         ]
#     },
# )
# @patch("databricks_cli.jobs.api.JobsService.run_now", return_value={"run_id": "1"})
# @patch(
#     "databricks_cli.jobs.api.JobsService.get_run",

# )
# def test_trace_runs(self, *_):
#     with self.project_dir:
#         ws_dir = "/Shared/dbx/projects/%s" % self.project_name
#         configure_result = invoke_cli_runner(
#             configure,
#             [
#                 "--environment",
#                 "test",
#                 "--profile",
#                 self.profile_name,
#                 "--workspace-dir",
#                 ws_dir,
#             ],
#         )
#         self.assertEqual(configure_result.exit_code, 0)
#
#         deployment_content = {"test": {"dbfs": {}, "jobs": []}}
#
#         JsonUtils.write(DEFAULT_DEPLOYMENT_FILE_PATH, deployment_content)
#
#         def _test():
#             deploy_result = invoke_cli_runner(deploy, ["--environment", "test", "--tags", "cake=cheesecake"])
#
#             self.assertEqual(deploy_result.exit_code, 0)
#
#             launch_result = invoke_cli_runner(
#                 launch, ["--environment", "test", "--job", "sample", "--tags", "cake=cheesecake", "--trace"]
#             )
#
#             self.assertEqual(launch_result.exit_code, 0)
#
#         self._launch_in_provided_context(_test)
#
def test_payload_keys():
    # here w check conversions towards API-based props
    nb_task = {"notebook_task": "something"}
    assert _define_payload_key(nb_task) == "notebook_params"

    sj_task = {"spark_jar_task": "something"}
    assert _define_payload_key(sj_task) == "jar_params"

    sp_task = {"spark_python_task": "something"}
    assert _define_payload_key(sp_task) == "python_params"
    ssb_task = {"spark_submit_task": "something"}
    assert _define_payload_key(ssb_task) == "spark_submit_params"
    with pytest.raises(Exception):
        _define_payload_key({})
