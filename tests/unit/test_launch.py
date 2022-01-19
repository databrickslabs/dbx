import base64
import datetime as dt
import json
import unittest
from unittest.mock import patch

import pandas as pd
from mlflow import ActiveRun
from mlflow.entities import Experiment
from mlflow.entities.run import Run, RunInfo, RunData

from dbx.commands.configure import configure
from dbx.commands.deploy import deploy
from dbx.commands.launch import launch, _define_payload_key
from dbx.utils.common import write_json, DEFAULT_DEPLOYMENT_FILE_PATH
from .utils import DbxTest, invoke_cli_runner, test_dbx_config

run_info = RunInfo(
    run_uuid="1",
    experiment_id="1",
    user_id="dbx",
    status="STATUS",
    start_time=dt.datetime.now(),
    end_time=dt.datetime.now(),
    lifecycle_stage="STAGE",
)
run_data = RunData()
run_mock = ActiveRun(Run(run_info, run_data))

DEFAULT_DATA_MOCK = {"data": base64.b64encode(json.dumps({"sample": "1"}).encode("utf-8"))}
RUN_SUBMIT_DATA_MOCK = {"data": base64.b64encode(json.dumps({"test": {"jobs": [{"name": "sample"}]}}).encode("utf-8"))}


class LaunchTest(DbxTest):
    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch("mlflow.search_runs", return_value=pd.DataFrame([{"run_id": 1, "tags.cake": "cheesecake"}]))
    @patch("databricks_cli.dbfs.api.DbfsService.read", return_value=DEFAULT_DATA_MOCK)
    @patch("databricks_cli.jobs.api.JobsService.list_runs", return_value={"runs": []})
    @patch(
        "databricks_cli.jobs.api.JobsService.list_jobs",
        return_value={
            "jobs": [
                {
                    "settings": {
                        "name": "sample",
                    },
                    "job_id": 1,
                }
            ]
        },
    )
    @patch("databricks_cli.jobs.api.JobsService.run_now", return_value={"run_id": "1"})
    @patch("databricks_cli.jobs.api.JobsService.get_run", return_value={"run_id": "1", "run_page_url": "http://some"})
    def test_launch(self, *_):
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            configure_result = invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "test",
                    "--profile",
                    self.profile_name,
                    "--workspace-dir",
                    ws_dir,
                ],
            )
            self.assertEqual(configure_result.exit_code, 0)

            deployment_content = {"test": {"dbfs": {}, "jobs": []}}

            write_json(deployment_content, DEFAULT_DEPLOYMENT_FILE_PATH)

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(deploy, ["--environment", "test", "--tags", "cake=cheesecake"])

                self.assertEqual(deploy_result.exit_code, 0)

                launch_result = invoke_cli_runner(
                    launch,
                    [
                        "--environment",
                        "test",
                        "--job",
                        "sample",
                        "--tags",
                        "cake=cheesecake",
                        "--branch-name",
                        "test-branch",
                    ],
                )

                self.assertEqual(launch_result.exit_code, 0)

    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch(
        "mlflow.search_runs",
        return_value=pd.DataFrame([{"run_id": 1, "tags.cake": "cheesecake", "tags.dbx_deploy_type": "files_only"}]),
    )
    @patch("databricks_cli.dbfs.api.DbfsService.read", return_value=RUN_SUBMIT_DATA_MOCK)
    @patch("databricks_cli.jobs.api.JobsService.list_runs", return_value={"runs": []})
    @patch("dbx.commands.launch._submit_run", return_value={"run_id": 1})
    @patch("databricks_cli.jobs.api.JobsService.get_run", return_value={"run_id": "1", "run_page_url": "http://some"})
    def test_launch_run_submit(self, *_):
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            configure_result = invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "test",
                    "--profile",
                    self.profile_name,
                    "--workspace-dir",
                    ws_dir,
                ],
            )
            self.assertEqual(configure_result.exit_code, 0)

            deployment_content = {"test": {"dbfs": {}, "jobs": []}}

            write_json(deployment_content, DEFAULT_DEPLOYMENT_FILE_PATH)

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(deploy, ["--environment", "test", "--tags", "cake=cheesecake"])

                self.assertEqual(deploy_result.exit_code, 0)

                launch_result = invoke_cli_runner(
                    launch,
                    ["--environment", "test", "--job", "sample", "--tags", "cake=cheesecake", "--as-run-submit"],
                )

                self.assertEqual(launch_result.exit_code, 0)

    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch("mlflow.search_runs", return_value=pd.DataFrame([{"run_id": 1, "tags.cake": "cheesecake"}]))
    @patch("databricks_cli.dbfs.api.DbfsService.read", return_value=DEFAULT_DATA_MOCK)
    @patch("databricks_cli.jobs.api.JobsService.list_runs", return_value={"runs": []})
    @patch(
        "databricks_cli.jobs.api.JobsService.list_jobs",
        return_value={
            "jobs": [
                {
                    "settings": {
                        "name": "sample",
                        "spark_python_task": {
                            "python_file": "some.entrypoint.py",
                        },
                    },
                    "job_id": 1,
                }
            ]
        },
    )
    @patch("databricks_cli.jobs.api.JobsService.run_now", return_value={"run_id": "1"})
    @patch("databricks_cli.jobs.api.JobsService.get_run", return_value={"run_id": "1", "run_page_url": "http://some"})
    def test_launch_with_parameters(self, *_):
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            configure_result = invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "test",
                    "--profile",
                    self.profile_name,
                    "--workspace-dir",
                    ws_dir,
                ],
            )
            self.assertEqual(configure_result.exit_code, 0)

            deployment_content = {"test": {"dbfs": {}, "jobs": []}}

            write_json(deployment_content, DEFAULT_DEPLOYMENT_FILE_PATH)

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(deploy, ["--environment", "test", "--tags", "cake=cheesecake"])

                self.assertEqual(deploy_result.exit_code, 0)

                launch_raw = invoke_cli_runner(
                    launch,
                    [
                        "--environment",
                        "test",
                        "--job",
                        "sample",
                        "--tags",
                        "cake=cheesecake",
                        """--parameters-raw={"key1": "value1", "key2": 2}""",
                    ],
                )

                self.assertEqual(launch_raw.exit_code, 0)

                launch_with_params = invoke_cli_runner(
                    launch,
                    [
                        "--environment",
                        "test",
                        "--job",
                        "sample",
                        "--tags",
                        "cake=cheesecake",
                        "--parameters='cake=cheesecake'",
                    ],
                )

                self.assertEqual(launch_with_params.exit_code, 0)

    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch(
        "mlflow.search_runs",
        return_value=pd.DataFrame([{"run_id": 1, "tags.cake": "strudel", "tags.dbx_deploy_type": "files_only"}]),
    )
    @patch("databricks_cli.dbfs.api.DbfsService.read", return_value=RUN_SUBMIT_DATA_MOCK)
    @patch("databricks_cli.jobs.api.JobsService.list_runs", return_value={"runs": []})
    @patch("dbx.commands.launch._submit_run", return_value={"run_id": 1})
    @patch("databricks_cli.jobs.api.JobsService.get_run", return_value={"run_id": "1", "run_page_url": "http://some"})
    def test_no_runs_run_submit(self, *_):
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            configure_result = invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "test",
                    "--profile",
                    self.profile_name,
                    "--workspace-dir",
                    ws_dir,
                ],
            )
            self.assertEqual(configure_result.exit_code, 0)

            deployment_content = {"test": {"dbfs": {}, "jobs": []}}

            write_json(deployment_content, DEFAULT_DEPLOYMENT_FILE_PATH)

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(deploy, ["--environment", "test", "--tags", "cake=cheesecake"])

                self.assertEqual(deploy_result.exit_code, 0)

                launch_result = invoke_cli_runner(
                    launch,
                    ["--environment", "test", "--job", "sample", "--tags", "cake=cheesecake", "--as-run-submit"],
                    expected_error=True,
                )

                self.assertIsNotNone(launch_result.exception)
                self.assertTrue("No deployments provided per given set of filters:" in str(launch_result.exception))

    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch("mlflow.search_runs", return_value=pd.DataFrame([]))
    @patch("databricks_cli.dbfs.api.DbfsService.read", return_value=DEFAULT_DATA_MOCK)
    @patch("databricks_cli.jobs.api.JobsService.list_runs", return_value={"runs": []})
    @patch("databricks_cli.jobs.api.JobsService.run_now", return_value={"run_id": "1"})
    def test_no_runs(self, *_):
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            configure_result = invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "test",
                    "--profile",
                    self.profile_name,
                    "--workspace-dir",
                    ws_dir,
                ],
            )
            self.assertEqual(configure_result.exit_code, 0)

            deployment_content = {"test": {"dbfs": {}, "jobs": []}}

            write_json(deployment_content, DEFAULT_DEPLOYMENT_FILE_PATH)

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(deploy, ["--environment", "test", "--tags", "cake=cheesecake"])

                self.assertEqual(deploy_result.exit_code, 0)

                launch_result = invoke_cli_runner(
                    launch,
                    [
                        "--environment",
                        "test",
                        "--job",
                        "sample",
                        "--tags",
                        "cake=cheesecake",
                    ],
                    expected_error=True,
                )

                self.assertIsNotNone(launch_result.exception)
                self.assertTrue("not found in underlying MLflow experiment" in str(launch_result.exception))

    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch("mlflow.search_runs", return_value=pd.DataFrame([{"run_id": 1, "tags.cake": "cheesecake"}]))
    @patch("databricks_cli.dbfs.api.DbfsService.read", return_value=DEFAULT_DATA_MOCK)
    @patch("databricks_cli.jobs.api.JobsService.list_runs", return_value={"runs": []})
    @patch(
        "databricks_cli.jobs.api.JobsService.list_jobs",
        return_value={
            "jobs": [
                {
                    "settings": {
                        "name": "sample",
                    },
                    "job_id": 1,
                }
            ]
        },
    )
    @patch("databricks_cli.jobs.api.JobsService.run_now", return_value={"run_id": "1"})
    @patch(
        "databricks_cli.jobs.api.JobsService.get_run",
        side_effect=[
            {"run_id": "1", "run_page_url": "http://some", "state": {"state_message": "RUNNING", "result_state": None}},
            {"run_id": "1", "run_page_url": "http://some", "state": {"state_message": "RUNNING", "result_state": None}},
            {
                "run_id": "1",
                "run_page_url": "http://some",
                "state": {"state_message": "RUNNING", "life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
            },
        ],
    )
    def test_trace_runs(self, *_):
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            configure_result = invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "test",
                    "--profile",
                    self.profile_name,
                    "--workspace-dir",
                    ws_dir,
                ],
            )
            self.assertEqual(configure_result.exit_code, 0)

            deployment_content = {"test": {"dbfs": {}, "jobs": []}}

            write_json(deployment_content, DEFAULT_DEPLOYMENT_FILE_PATH)

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(deploy, ["--environment", "test", "--tags", "cake=cheesecake"])

                self.assertEqual(deploy_result.exit_code, 0)

                launch_result = invoke_cli_runner(
                    launch, ["--environment", "test", "--job", "sample", "--tags", "cake=cheesecake", "--trace"]
                )

                self.assertEqual(launch_result.exit_code, 0)

    def test_payload_keys(self, *_):
        # here w check conversions towards API-based props
        nb_task = {"notebook_task": "something"}
        self.assertEqual(_define_payload_key(nb_task), "notebook_params")

        sj_task = {"spark_jar_task": "something"}
        self.assertEqual(_define_payload_key(sj_task), "jar_params")

        sp_task = {"spark_python_task": "something"}
        self.assertEqual(_define_payload_key(sp_task), "python_params")
        ssb_task = {"spark_submit_task": "something"}
        self.assertEqual(_define_payload_key(ssb_task), "spark_submit_params")
        self.assertRaises(Exception, _define_payload_key, {})


if __name__ == "__main__":
    unittest.main()
