import datetime as dt
import pathlib
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
    def test_deploy_basic(self, *args):
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

    @patch("databricks_cli.configure.provider.ProfileConfigProvider.get_config",
           return_value=test_dbx_config)
    @patch("databricks_cli.workspace.api.WorkspaceService.get_status", return_value=True)
    @patch("mlflow.get_experiment_by_name", return_value=Experiment("id", None, "location", None, None))
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    def test_deploy_non_existent_env(self, *args):
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
                "--environment", "non-existent"
            ], expected_error=True)

            self.assertEqual(deploy_result.exit_code, 1)

    @patch("databricks_cli.configure.provider.ProfileConfigProvider.get_config",
           return_value=test_dbx_config)
    @patch("databricks_cli.workspace.api.WorkspaceService.get_status", return_value=True)
    @patch("databricks_cli.jobs.api.JobsService.list_jobs", return_value={"jobs": []})
    @patch("databricks_cli.jobs.api.JobsApi.create_job", return_value={"job_id": "1"})
    @patch("mlflow.get_experiment_by_name", return_value=Experiment("id", None, "location", None, None))
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    def test_deploy_listed_jobs(self, *args):
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
                    "jobs": [{"name": "job-1"}, {"name": "job-2"}]
                }
            }

            write_json(deployment_content, DEFAULT_DEPLOYMENT_FILE_PATH)

            deploy_result = invoke_cli_runner(deploy, [
                "--environment", "test",
                "--jobs", "job-1,job-2"
            ])

            self.assertEqual(deploy_result.exit_code, 0)

    @patch("databricks_cli.configure.provider.ProfileConfigProvider.get_config",
           return_value=test_dbx_config)
    @patch("databricks_cli.workspace.api.WorkspaceService.get_status", return_value=True)
    @patch("databricks_cli.jobs.api.JobsService.list_jobs", return_value={"jobs": []})
    @patch("databricks_cli.jobs.api.JobsApi.create_job", return_value={"job_id": "1"})
    @patch("mlflow.get_experiment_by_name", return_value=Experiment("id", None, "location", None, None))
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    def test_deploy_with_requirements(self, *args):
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

            sample_reqs = "\n".join([
                "pyspark=3.0.0",
                "xgboost=0.6.0"
            ])

            pathlib.Path("runtime_requirements.txt").write_text(sample_reqs)

            deploy_result = invoke_cli_runner(deploy, [
                "--environment", "test",
                "--requirements", "runtime_requirements.txt"
            ])

            self.assertEqual(deploy_result.exit_code, 0)


if __name__ == '__main__':
    unittest.main()
