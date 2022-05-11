import datetime as dt
import os
import pathlib
import shutil
import unittest
from unittest.mock import patch, Mock

import yaml
from databricks_cli.sdk import JobsService
from mlflow import ActiveRun
from mlflow.entities import Experiment
from mlflow.entities.run import Run, RunInfo, RunData
from requests import HTTPError

from dbx.commands.configure import configure
from dbx.commands.deploy import deploy, _update_job  # noqa
from dbx.constants import INFO_FILE_PATH
from dbx.utils.json import JsonUtils
from .utils import DbxTest, invoke_cli_runner, test_dbx_config, DEFAULT_DEPLOYMENT_FILE_PATH
from .test_common import format_path

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
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.tracking.fluent.end_run", return_value=None)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch("databricks_cli.configure.config._get_api_client", return_value=None)
    def test_deploy_with_jobs(self, *_):
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

            JsonUtils.write(DEFAULT_DEPLOYMENT_FILE_PATH, deployment_content)

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
    @patch("databricks_cli.jobs.api.JobsService.list_jobs", return_value={"jobs": []})
    @patch("databricks_cli.jobs.api.JobsApi.create_job", return_value={"job_id": "1"})
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.tracking.fluent.end_run", return_value=None)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch("databricks_cli.configure.config._get_api_client", return_value=None)
    def test_deploy_multitask_json(self, *_):
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

            samples_path = pathlib.Path(format_path("../deployment-configs/"))
            deployment_content = JsonUtils.read(samples_path / "03-multitask-job.json")

            JsonUtils.write(DEFAULT_DEPLOYMENT_FILE_PATH, deployment_content)

            shutil.copy(samples_path / "placeholder_1.py", pathlib.Path("./placeholder_1.py"))
            shutil.copy(samples_path / "placeholder_2.py", pathlib.Path("./placeholder_2.py"))

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(
                    deploy, ["--environment", "default", "--write-specs-to-file", ".dbx/deployment-result.json"]
                )
                _content = JsonUtils.read(pathlib.Path(".dbx/deployment-result.json"))
                self.assertNotIn("libraries", _content["default"]["jobs"][0])
                self.assertIn("libraries", _content["default"]["jobs"][0]["tasks"][0])
                self.assertEqual(deploy_result.exit_code, 0)

    @patch("databricks_cli.sdk.service.DbfsService.get_status", return_value=None)
    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.jobs.api.JobsService.list_jobs", return_value={"jobs": []})
    @patch("databricks_cli.jobs.api.JobsApi.create_job", return_value={"job_id": "1"})
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.tracking.fluent.end_run", return_value=None)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch("databricks_cli.configure.config._get_api_client", return_value=None)
    def test_deploy_multitask_yaml(self, *_):
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

            samples_path = pathlib.Path(format_path("../deployment-configs/"))
            shutil.copy(samples_path / "03-multitask-job.yaml", pathlib.Path("./deployment.yml"))
            shutil.copy(samples_path / "placeholder_1.py", pathlib.Path("./placeholder_1.py"))
            shutil.copy(samples_path / "placeholder_2.py", pathlib.Path("./placeholder_2.py"))

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(
                    deploy,
                    [
                        "--environment",
                        "default",
                        "--deployment-file",
                        "deployment.yml",
                        "--write-specs-to-file",
                        ".dbx/deployment-result.json",
                    ],
                )
                _content = JsonUtils.read(pathlib.Path(".dbx/deployment-result.json"))
                self.assertNotIn("libraries", _content["default"]["jobs"][0])
                self.assertIn("libraries", _content["default"]["jobs"][0]["tasks"][0])
                self.assertEqual(deploy_result.exit_code, 0)

    @patch("databricks_cli.sdk.service.DbfsService.get_status", return_value=None)
    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.jobs.api.JobsService.list_jobs", return_value={"jobs": []})
    @patch("databricks_cli.jobs.api.JobsApi.create_job", return_value={"job_id": "1"})
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.tracking.fluent.end_run", return_value=None)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch("databricks_cli.configure.config._get_api_client", return_value=None)
    def test_deploy_path_adjustment_json(self, *_):
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

            samples_path = pathlib.Path(format_path("../deployment-configs/"))

            shutil.copy(samples_path / "04-path-adjustment-policy.json", pathlib.Path("./conf/deployment.json"))
            shutil.copy(samples_path / "placeholder_1.py", pathlib.Path("./placeholder_1.py"))
            shutil.copy(samples_path / "placeholder_2.py", pathlib.Path("./placeholder_2.py"))

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(
                    deploy, ["--environment", "default", "--write-specs-to-file", ".dbx/deployment-result.json"]
                )
                _content = JsonUtils.read(pathlib.Path(".dbx/deployment-result.json"))
                self.assertTrue(
                    _content["default"]["jobs"][0]["libraries"][0]["whl"].startswith("dbfs:/Shared/dbx-testing")
                )
                self.assertTrue(
                    _content["default"]["jobs"][0]["spark_python_task"]["python_file"].startswith(
                        "dbfs:/Shared/dbx-testing"
                    )
                )
                self.assertTrue(
                    _content["default"]["jobs"][0]["spark_python_task"]["parameters"][0].startswith(
                        "/dbfs/Shared/dbx-testing"
                    )
                )
                self.assertEqual(deploy_result.exit_code, 0)

    @patch("databricks_cli.sdk.service.DbfsService.get_status", return_value=None)
    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.jobs.api.JobsService.list_jobs", return_value={"jobs": []})
    @patch("databricks_cli.jobs.api.JobsApi.create_job", return_value={"job_id": "1"})
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.tracking.fluent.end_run", return_value=None)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch("databricks_cli.configure.config._get_api_client", return_value=None)
    def test_deploy_path_adjustment_yaml(self, *_):
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

            samples_path = pathlib.Path(format_path("../deployment-configs/"))

            shutil.copy(samples_path / "04-path-adjustment-policy.yaml", pathlib.Path("./conf/deployment.yml"))
            shutil.copy(samples_path / "placeholder_1.py", pathlib.Path("./placeholder_1.py"))
            shutil.copy(samples_path / "placeholder_2.py", pathlib.Path("./placeholder_2.py"))

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(
                    deploy,
                    [
                        "--environment",
                        "default",
                        "--write-specs-to-file",
                        ".dbx/deployment-result.json",
                    ],
                )

                _content = JsonUtils.read(pathlib.Path(".dbx/deployment-result.json"))

                self.assertTrue(
                    _content["default"]["jobs"][0]["libraries"][0]["whl"].startswith("dbfs:/Shared/dbx-testing")
                )
                self.assertTrue(
                    _content["default"]["jobs"][0]["spark_python_task"]["python_file"].startswith(
                        "dbfs:/Shared/dbx-testing"
                    )
                )
                self.assertTrue(
                    _content["default"]["jobs"][0]["spark_python_task"]["parameters"][0].startswith(
                        "/dbfs/Shared/dbx-testing"
                    )
                )
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
    @patch("mlflow.tracking.fluent.end_run", return_value=None)
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
                JsonUtils.write(DEFAULT_DEPLOYMENT_FILE_PATH, deployment_content)

                sample_config = JsonUtils.read(INFO_FILE_PATH)
                sample_config["environments"]["test"]["artifact_location"] = "dbfs:/some/another-location"
                JsonUtils.write(INFO_FILE_PATH, sample_config)

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
    @patch("mlflow.tracking.fluent.end_run", return_value=None)
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

            JsonUtils.write(DEFAULT_DEPLOYMENT_FILE_PATH, deployment_content)

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
    @patch("mlflow.tracking.fluent.end_run", return_value=None)
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

            JsonUtils.write(DEFAULT_DEPLOYMENT_FILE_PATH, deployment_content)

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result_jobs = invoke_cli_runner(deploy, ["--environment", "test", "--jobs", "job-1,job-2"])
                deploy_result_job = invoke_cli_runner(deploy, ["--environment", "test", "--job", "job-1"])

                deploy_result_both = invoke_cli_runner(
                    deploy, ["--environment", "test", "--job", "job-1", "--jobs", "job-1,job-2"], expected_error=True
                )

                self.assertEqual(deploy_result_jobs.exit_code, 0)
                self.assertEqual(deploy_result_job.exit_code, 0)
                self.assertRaises(Exception, deploy_result_both)

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
    @patch("mlflow.tracking.fluent.end_run", return_value=None)
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

            JsonUtils.write(DEFAULT_DEPLOYMENT_FILE_PATH, deployment_content)

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
    @patch("mlflow.tracking.fluent.end_run", return_value=None)
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
                    deploy,
                    [
                        "--deployment-file",
                        "conf/deployment.yml",
                        "--environment",
                        "default",
                        "--write-specs-to-file",
                        spec_file,
                    ],
                )

                self.assertEqual(deploy_result.exit_code, 0)

                spec_result = JsonUtils.read(pathlib.Path(spec_file))

                self.assertIsNotNone(spec_result)

                deploy_overwrite = invoke_cli_runner(
                    deploy,
                    [
                        "--deployment-file",
                        "conf/deployment.yml",
                        "--environment",
                        "default",
                        "--write-specs-to-file",
                        spec_file,
                    ],
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
    @patch("mlflow.tracking.fluent.end_run", return_value=None)
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

            deployment_file = pathlib.Path("conf/deployment.yml")
            deploy_content = yaml.safe_load(deployment_file.read_text())

            sample_job = deploy_content.get("environments").get("default").get("jobs")[0]

            sample_job["permissions"] = {
                "access_control_list": [
                    {
                        "user_name": "some_user@example.com",
                        "permission_level": "IS_OWNER",
                    },
                    {"group_name": "some-user-group", "permission_level": "CAN_VIEW"},
                ]
            }

            deployment_file.write_text(yaml.safe_dump(deploy_content))

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/dbx/{self.project_name}", None, None),
            ):
                deploy_result = invoke_cli_runner(
                    deploy, ["--deployment-file", "conf/deployment.yml", "--environment", "default"]
                )

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
    @patch("databricks_cli.jobs.api.JobsService.list_jobs", return_value={"jobs": []})
    @patch("databricks_cli.jobs.api.JobsApi.create_job", return_value={"job_id": "1"})
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("mlflow.set_experiment", return_value=None)
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.tracking.fluent.end_run", return_value=None)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch("databricks_cli.configure.config._get_api_client", return_value=None)
    @patch("mlflow.set_experiment", return_value=None)
    @patch.dict(os.environ, {"DATABRICKS_HOST": "", "DATABRICKS_TOKEN": "some-fake-token"})
    def test_deployment_with_bad_env_variable(self, *_):
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

            samples_path = pathlib.Path(format_path("../deployment-configs/"))
            deployment_content = JsonUtils.read(samples_path / "03-multitask-job.json")

            JsonUtils.write(DEFAULT_DEPLOYMENT_FILE_PATH, deployment_content)

            shutil.copy(samples_path / "placeholder_1.py", pathlib.Path("./placeholder_1.py"))
            shutil.copy(samples_path / "placeholder_2.py", pathlib.Path("./placeholder_2.py"))

            deploy_result = invoke_cli_runner(
                deploy,
                ["--environment", "default", "--write-specs-to-file", ".dbx/deployment-result.json"],
                expected_error=True,
            )
            self.assertIsInstance(deploy_result.exception, IndexError)


if __name__ == "__main__":
    unittest.main()
