import logging
import shutil
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

from databricks_cli.configure.provider import DatabricksConfig
from mlflow.entities import Experiment
from path import Path
from requests.exceptions import HTTPError

from dbx.cli.configure import configure
from dbx.cli.utils import InfoFile
from .utils import initialize_cookiecutter, invoke_cli_runner

TEST_HOST = "https:/dbx.cloud.databricks.com"
TEST_TOKEN = "dapiDBXTEST"


class ConfigureTest(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp()
        self.project_name = "dev_dbx_aws_%s" % str(uuid4()).split("-")[0]
        self.profile_name = "dbx-test"
        logging.info(
            "Launching configure test in directory %s with project name %s" % (self.test_dir, self.project_name))
        with Path(self.test_dir):
            initialize_cookiecutter(self.project_name)

        self.project_dir = Path(self.test_dir).joinpath(self.project_name)

    @patch("databricks_cli.configure.provider.ProfileConfigProvider.get_config",
           return_value=DatabricksConfig.from_token(TEST_HOST, TEST_TOKEN))
    @patch("databricks_cli.workspace.api.WorkspaceService.get_status", return_value=True)
    @patch("mlflow.get_experiment_by_name", return_value=Experiment("id", None, "location", None, None))
    def test_configure_with_existing_path(self, *args) -> None:
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            first_result = invoke_cli_runner(configure, [
                "--name", "test",
                "--profile", self.profile_name,
                "--workspace-dir", ws_dir
            ])

            self.assertEquals(first_result.exit_code, 0)

            env = InfoFile.get("environments").get("test")
            self.assertIsNotNone(env)
            self.assertEquals(env["profile"], self.profile_name)
            self.assertEquals(env["workspace_dir"], ws_dir)
            self.assertIsNotNone(env["experiment_id"])
            self.assertIsNotNone(env["artifact_location"])

            # repeating test for the second time to check if configure raises error with the same arguments
            second_result = invoke_cli_runner(configure, [
                "--name", "test",
                "--profile", self.profile_name,
                "--workspace-dir", ws_dir
            ], expected_error=True)

            self.assertEquals(second_result.exit_code, 1)

    @patch("databricks_cli.configure.provider.ProfileConfigProvider.get_config",
           return_value=DatabricksConfig.from_token(TEST_HOST, TEST_TOKEN))
    @patch("databricks_cli.workspace.api.WorkspaceService.get_status", side_effect=HTTPError())
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value={"status": "fine"})
    @patch("mlflow.get_experiment_by_name", return_value=Experiment("id", None, "location", None, None))
    def test_configure_with_mkdirs(self, *args) -> None:
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            first_result = invoke_cli_runner(configure, [
                "--name", "mkdirs-test",
                "--profile", self.profile_name,
                "--workspace-dir", ws_dir
            ])
            self.assertEquals(first_result.exit_code, 0)

    @patch("databricks_cli.configure.provider.ProfileConfigProvider.get_config",
           return_value=DatabricksConfig.from_token(TEST_HOST, TEST_TOKEN))
    @patch("databricks_cli.workspace.api.WorkspaceService.get_status", return_value=True)
    # we return none on the first function call, and experiment on the second function call
    @patch("mlflow.get_experiment_by_name", side_effect=[None, Experiment("123", None, None, None)])
    @patch("mlflow.create_experiment", return_value=True)
    def test_configure_create_experiment(self, *args) -> None:
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            first_result = invoke_cli_runner(configure, [
                "--name", "mkdirs-test",
                "--profile", self.profile_name,
                "--workspace-dir", ws_dir
            ])
            self.assertEquals(first_result.exit_code, 0)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir)


if __name__ == '__main__':
    unittest.main()
