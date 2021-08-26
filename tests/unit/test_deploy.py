import datetime as dt
import json
import pathlib
import unittest
from unittest.mock import patch, Mock

from databricks_cli.sdk import JobsService
from mlflow import ActiveRun
from mlflow.entities import Experiment
from mlflow.entities.run import Run, RunInfo, RunData
from requests import HTTPError

from dbx.commands.configure import configure
from dbx.commands.deploy import deploy, _update_job  # noqa
from dbx.utils.common import write_json, DEFAULT_DEPLOYMENT_FILE_PATH, read_json, INFO_FILE_PATH
from .utils import DbxTest, invoke_cli_runner, test_dbx_config

run_info = RunInfo(
    run_uuid="1",
    experiment_id="1",
    user_id="dbx",
    status="STATUS",
    start_time=dt.datetime.now(),
    end_time=dt.datetime.now(),
    lifecycle_stage="STAGE",
    artifact_uri="dbfs:/Shared/dbx-testing",
)
run_data = RunData()
run_mock = ActiveRun(Run(run_info, run_data))


class DeployTest(DbxTest):
    @patch("databricks_cli.sdk.service.DbfsService.get_status", return_value=None)
    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch("databricks_cli.configure.config._get_api_client", return_value=None)
    @patch("mlflow.set_experiment", return_value=None)
    def test_deploy_basic(self, *_):
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

            deployment_content = {"test": {"jobs": []}}

            write_json(deployment_content, DEFAULT_DEPLOYMENT_FILE_PATH)

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(deploy, ["--environment", "test"])
                self.assertEqual(deploy_result.exit_code, 0)

    @patch("databricks_cli.sdk.service.DbfsService.get_status", return_value=None)
    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch("databricks_cli.configure.config._get_api_client", return_value=None)
    @patch("mlflow.set_experiment", return_value=None)
    def test_deploy_incorrect_artifact_location(self, *_):
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

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, "dbfs:/some/correct-location", None, None),
            ):
                deployment_content = {"test": {"jobs": []}}
                write_json(deployment_content, DEFAULT_DEPLOYMENT_FILE_PATH)

                sample_config = read_json(INFO_FILE_PATH)
                sample_config["environments"]["test"]["artifact_location"] = "dbfs:/some/another-location"
                write_json(sample_config, INFO_FILE_PATH)

                deploy_result = invoke_cli_runner(deploy, ["--environment", "test"], expected_error=True)

                self.assertIsInstance(deploy_result.exception, Exception)
                self.assertIn(
                    "Please change the experiment name to create a new experiment", str(deploy_result.exception)
                )

    @patch("databricks_cli.sdk.service.DbfsService.get_status", return_value=None)
    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("databricks_cli.workspace.api.WorkspaceService.get_status", return_value=True)
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    def test_deploy_non_existent_env(self, *_):
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

            deployment_content = {"misconfigured-environment": {"dbfs": {}, "jobs": []}}

            write_json(deployment_content, DEFAULT_DEPLOYMENT_FILE_PATH)

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(deploy, ["--environment", "test"], expected_error=True)

                self.assertIsInstance(deploy_result.exception, NameError)
                self.assertIn("non-existent in the deployment file", str(deploy_result.exception))

    @patch("databricks_cli.sdk.service.DbfsService.get_status", return_value=None)
    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("databricks_cli.workspace.api.WorkspaceService.get_status", return_value=True)
    @patch("databricks_cli.jobs.api.JobsService.list_jobs", return_value={"jobs": []})
    @patch("databricks_cli.jobs.api.JobsApi.create_job", return_value={"job_id": "1"})
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    def test_deploy_listed_jobs(self, *_):
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

            deployment_content = {"test": {"jobs": [{"name": "job-1"}, {"name": "job-2"}]}}

            write_json(deployment_content, DEFAULT_DEPLOYMENT_FILE_PATH)

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(deploy, ["--environment", "test", "--jobs", "job-1,job-2"])
                self.assertEqual(deploy_result.exit_code, 0)

    @patch("databricks_cli.sdk.service.DbfsService.get_status", return_value=None)
    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("databricks_cli.workspace.api.WorkspaceService.get_status", return_value=True)
    @patch("databricks_cli.jobs.api.JobsService.list_jobs", return_value={"jobs": []})
    @patch("databricks_cli.jobs.api.JobsApi.create_job", return_value={"job_id": "1"})
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    def test_deploy_with_requirements_and_branch(self, *_):
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

            deployment_content = {"test": {"jobs": []}}

            write_json(deployment_content, DEFAULT_DEPLOYMENT_FILE_PATH)

            sample_requirements = "\n".join(["pyspark=3.0.0", "xgboost=0.6.0", "pyspark3d"])

            pathlib.Path("runtime_requirements.txt").write_text(sample_requirements)

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(
                    deploy,
                    [
                        "--environment",
                        "test",
                        "--requirements-file",
                        "runtime_requirements.txt",
                        "--branch-name",
                        "test-branch",
                    ],
                )

                deleted_dependency_lines = [
                    line for line in deploy_result.stdout.splitlines() if "pyspark dependency deleted" in line
                ]
                self.assertEqual(len(deleted_dependency_lines), 1)

                self.assertEqual(deploy_result.exit_code, 0)

    def test_update_job_positive(self):
        js = Mock(JobsService)
        _update_job(js, "aa-bbb-ccc-111", {"name": 1})
        self.assertEqual(0, 0)  # dummy test to verify positive case

    def test_update_job_negative(self):
        js = Mock(JobsService)
        js.reset_job.side_effect = Mock(side_effect=HTTPError())
        self.assertRaises(HTTPError, _update_job, js, "aa-bbb-ccc-111", {"name": 1})

    @patch("databricks_cli.sdk.service.DbfsService.get_status", return_value=None)
    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("databricks_cli.workspace.api.WorkspaceService.get_status", return_value=True)
    @patch("databricks_cli.jobs.api.JobsService.list_jobs", return_value={"jobs": []})
    @patch("databricks_cli.jobs.api.JobsApi.create_job", return_value={"job_id": "1"})
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    def test_write_specs_to_file(self, *_):
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            configure_result = invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "default",
                    "--profile",
                    self.profile_name,
                    "--workspace-dir",
                    ws_dir,
                ],
            )
            self.assertEqual(configure_result.exit_code, 0)

            spec_file = ".dbx/deployment-result.json"
            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(
                    deploy, ["--environment", "default", "--write-specs-to-file", spec_file]
                )

                self.assertEqual(deploy_result.exit_code, 0)

                spec_result = json.loads(pathlib.Path(spec_file).read_text())

                self.assertIsNotNone(spec_result)

                deploy_overwrite = invoke_cli_runner(
                    deploy, ["--environment", "default", "--write-specs-to-file", spec_file]
                )

                self.assertEqual(deploy_overwrite.exit_code, 0)

    @patch("databricks_cli.sdk.api_client.ApiClient.perform_query", return_value=None)
    @patch("databricks_cli.sdk.service.DbfsService.get_status", return_value=None)
    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("databricks_cli.workspace.api.WorkspaceService.get_status", return_value=True)
    @patch("databricks_cli.jobs.api.JobsService.list_jobs", return_value={"jobs": []})
    @patch("databricks_cli.jobs.api.JobsApi.create_job", return_value={"job_id": "1"})
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    def test_with_permissions(self, *_):
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            configure_result = invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "default",
                    "--profile",
                    self.profile_name,
                    "--workspace-dir",
                    ws_dir,
                ],
            )
            self.assertEqual(configure_result.exit_code, 0)

            deployment_file = pathlib.Path(DEFAULT_DEPLOYMENT_FILE_PATH)
            deploy_content = json.loads(deployment_file.read_text())

            sample_job = deploy_content.get("default").get("jobs")[0]

            sample_job["permissions"] = {
                "access_control_list": [
                    {
                        "user_name": "some_user@example.com",
                        "permission_level": "IS_OWNER",
                    },
                    {"group_name": "some-user-group", "permission_level": "CAN_VIEW"},
                ]
            }

            deployment_file.write_text(json.dumps(deploy_content, indent=4))

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(deploy, ["--environment", "default"])

                self.assertEqual(deploy_result.exit_code, 0)


if __name__ == "__main__":
    unittest.main()
