import unittest
from pathlib import Path

from dbx.commands.configure import configure
from dbx.constants import INFO_FILE_PATH
from dbx.models.project import MlflowArtifactStorageInfo
from dbx.utils.common import ProjectConfigurationManager
from .utils import invoke_cli_runner, DbxTest


class ConfigureTest(DbxTest):
    def test_configure_default(self, *args) -> None:
        with self.project_dir:
            Path(INFO_FILE_PATH).unlink()
            first_result = invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "test",
                ],
            )
            self.assertEqual(first_result.exit_code, 0)
            env: MlflowArtifactStorageInfo = ProjectConfigurationManager().get("test")
            self.assertIsNotNone(env)
            self.assertEqual(env.properties.artifact_location, f"dbfs:/dbx/{self.project_name}")
            self.assertEqual(env.properties.workspace_directory, f"/Shared/dbx/projects/{self.project_name}")

    def test_configure(self, *args) -> None:
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            artifact_location = f"dbfs:/dbx/custom-project-location/{self.project_name}"
            first_result = invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "test",
                    "-p",
                    f"workspace_directory={ws_dir}",
                    "-p",
                    f"artifact_location={artifact_location}",
                ],
            )

            self.assertEqual(first_result.exit_code, 0)

            env: MlflowArtifactStorageInfo = ProjectConfigurationManager().get("test")
            self.assertIsNotNone(env)
            self.assertEqual(env.properties.workspace_directory, ws_dir)

    def test_update_configuration(self, *args):
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "test",
                    "-p",
                    f"workspace_directory={ws_dir}",
                ],
            )

            new_ws_dir = ws_dir + "/updated"
            invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "test",
                    "-p",
                    f"workspace_directory={new_ws_dir}",
                ],
            )

            env: MlflowArtifactStorageInfo = ProjectConfigurationManager().get("test")
            self.assertIsNotNone(env)
            self.assertEqual(env.properties.workspace_directory, new_ws_dir)


if __name__ == "__main__":
    unittest.main()
