import base64
import datetime as dt
import json
import unittest
from unittest.mock import patch

import pandas as pd
from mlflow import ActiveRun
from mlflow.entities import Experiment
from mlflow.entities.run import Run, RunInfo, RunData

from dbx.cli.configure import configure
from dbx.cli.deploy import deploy
from dbx.cli.launch import launch
from dbx.cli.utils import write_json, DEFAULT_DEPLOYMENT_FILE_PATH
from .utils import DbxTest, invoke_cli_runner, test_dbx_config

run_info = RunInfo(
    run_uuid="1",
    experiment_id="1",
    user_id="dbx",
    status="STATUS",
    start_time=dt.datetime.now(),
    end_time=dt.datetime.now(),
    lifecycle_stage="STAGE")
run_data = RunData()
run_mock = ActiveRun(Run(run_info, run_data))

data_mock = {"data": base64.b64encode(json.dumps({"sample": "1"}).encode("utf-8"))}


class LaunchTest(DbxTest):
    @patch("databricks_cli.configure.provider.ProfileConfigProvider.get_config",
           return_value=test_dbx_config)
    @patch("databricks_cli.workspace.api.WorkspaceService.get_status", return_value=True)
    @patch("mlflow.get_experiment_by_name", return_value=Experiment("id", None, "location", None, None))
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
            configure_result = invoke_cli_runner(configure, [
                "--environment", "test",
                "--profile", self.profile_name,
                "--workspace-dir", ws_dir
            ])
            self.assertEqual(configure_result.exit_code, 0)

            deployment_content = {
                "test": {
                    "dbfs": {},
                    "jobs": []
                }
            }

            write_json(deployment_content, DEFAULT_DEPLOYMENT_FILE_PATH)

            deploy_result = invoke_cli_runner(deploy, [
                "--environment", "test"
            ])

            self.assertEqual(deploy_result.exit_code, 0)

            launch_result = invoke_cli_runner(launch, [
                "--environment", "test",
                "--job", "sample"
            ])

            self.assertEqual(launch_result.exit_code, 0)


if __name__ == '__main__':
    unittest.main()
