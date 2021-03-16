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
from dbx.commands.launch import launch
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

data_mock = {"data": base64.b64encode(json.dumps({"sample": "1"}).encode("utf-8"))}


class LaunchTest(DbxTest):
    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch(
        "mlflow.get_experiment_by_name",
        return_value=Experiment("id", None, "location", None, None),
    )
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch("mlflow.search_runs", return_value=pd.DataFrame([{"run_id": 1}]))
    @patch("databricks_cli.dbfs.api.DbfsService.read", return_value=data_mock)
    @patch("databricks_cli.jobs.api.JobsService.list_runs", return_value={"runs": []})
    @patch("databricks_cli.jobs.api.JobsService.run_now", return_value={"run_id": "1"})
    def test_launch(self, *args):
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
            )

            self.assertEqual(launch_result.exit_code, 0)

    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch(
        "mlflow.get_experiment_by_name",
        return_value=Experiment("id", None, "location", None, None),
    )
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch("mlflow.search_runs", return_value=pd.DataFrame([]))
    @patch("databricks_cli.dbfs.api.DbfsService.read", return_value=data_mock)
    @patch("databricks_cli.jobs.api.JobsService.list_runs", return_value={"runs": []})
    @patch("databricks_cli.jobs.api.JobsService.run_now", return_value={"run_id": "1"})
    def test_no_runs(self, *args):
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
            self.assertTrue("No runs provided per given set of filters" in str(launch_result.exception))

    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch(
        "mlflow.get_experiment_by_name",
        return_value=Experiment("id", None, "location", None, None),
    )
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch("mlflow.search_runs", return_value=pd.DataFrame([{"run_id": "1"}]))
    @patch("databricks_cli.dbfs.api.DbfsService.read", return_value=data_mock)
    @patch("databricks_cli.jobs.api.JobsService.list_runs", return_value={"runs": []})
    @patch("databricks_cli.jobs.api.JobsService.run_now", return_value={"run_id": "1"})
    @patch(
        "databricks_cli.jobs.api.JobsService.get_run",
        side_effect=[
            {"state": {"state_message": "RUNNING", "result_state": None}},
            {"state": {"state_message": "RUNNING", "result_state": None}},
            {"state": {"state_message": "RUNNING", "life_cycle_state": "TERMINATED", "result_state": "SUCCESS"}},
        ],
    )
    def test_trace_runs(self, *args):
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

            deploy_result = invoke_cli_runner(deploy, ["--environment", "test", "--tags", "cake=cheesecake"])

            self.assertEqual(deploy_result.exit_code, 0)

            launch_result = invoke_cli_runner(
                launch, ["--environment", "test", "--job", "sample", "--tags", "cake=cheesecake", "--trace"]
            )

            self.assertEqual(launch_result.exit_code, 0)


if __name__ == "__main__":
    unittest.main()
