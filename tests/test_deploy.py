import datetime as dt
import unittest
from unittest.mock import patch

from mlflow import ActiveRun
from mlflow.entities import Experiment
from mlflow.entities.run import Run, RunInfo, RunData

from dbx.cli.configure import configure
from dbx.cli.deploy import deploy
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


class DeployTest(DbxTest):
    @patch("databricks_cli.configure.provider.ProfileConfigProvider.get_config",
           return_value=test_dbx_config)
    @patch("databricks_cli.workspace.api.WorkspaceService.get_status", return_value=True)
    @patch("mlflow.get_experiment_by_name", return_value=Experiment("id", None, "location", None, None))
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    def test_deploy(self, *args):
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            configure_result = invoke_cli_runner(configure, [
                "--name", "test",
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


if __name__ == '__main__':
    unittest.main()
