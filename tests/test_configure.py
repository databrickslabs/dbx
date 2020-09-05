import unittest
from unittest.mock import patch

from databricks_cli.configure.provider import DatabricksConfig
from mlflow.entities import Experiment
from requests.exceptions import HTTPError

from dbx.cli.configure import configure
from dbx.cli.utils import InfoFile
from .utils import invoke_cli_runner, DbxTest

TEST_HOST = "https:/dbx.cloud.databricks.com"
TEST_TOKEN = "dapiDBXTEST"

"""
What do we test: dbx configure
Expected behaviour: if no .dbx folder provided -> create folder, initialize InfoFile, create environment
"""


class ConfigureTest(DbxTest):

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

            self.assertEqual(first_result.exit_code, 0)

            env = InfoFile.get("environments").get("test")
            self.assertIsNotNone(env)
            self.assertEqual(env["profile"], self.profile_name)
            self.assertEqual(env["workspace_dir"], ws_dir)
            self.assertIsNotNone(env["experiment_id"])
            self.assertIsNotNone(env["artifact_location"])

            # repeating test for the second time to check if configure raises error with the same arguments
            second_result = invoke_cli_runner(configure, [
                "--name", "test",
                "--profile", self.profile_name,
                "--workspace-dir", ws_dir
            ], expected_error=True)

            self.assertEqual(second_result.exit_code, 1)

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
            self.assertEqual(first_result.exit_code, 0)

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
                "--name", "create-experiment-test",
                "--profile", self.profile_name,
                "--workspace-dir", ws_dir
            ])
            self.assertEqual(first_result.exit_code, 0)


if __name__ == '__main__':
    unittest.main()
