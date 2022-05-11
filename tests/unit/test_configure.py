import unittest

from dbx.commands.configure import configure
from dbx.utils.common import ConfigurationManager
from dbx.constants import INFO_FILE_PATH
from .utils import invoke_cli_runner, DbxTest
from pathlib import Path


class ConfigureTest(DbxTest):
    def test_configure_default(self, *args) -> None:
        with self.project_dir:
            Path(INFO_FILE_PATH).unlink()
            first_result = invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "test",
                    "--profile",
                    self.profile_name,
                ],
            )
            self.assertEqual(first_result.exit_code, 0)
            env = ConfigurationManager().get("test")
            self.assertIsNotNone(env)
            self.assertEqual(env.profile, self.profile_name)
            self.assertEqual(env.artifact_location, f"dbfs:/dbx/{self.project_name}")
            self.assertEqual(env.workspace_dir, f"/Shared/dbx/projects/{self.project_name}")

    def test_configure(self, *args) -> None:
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            first_result = invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "test",
                    "--profile",
                    self.profile_name,
                    "--workspace-dir",
                    ws_dir,
                    "--artifact-location",
                    f"dbfs:/dbx/custom-project-location/{self.project_name}",
                ],
            )

            self.assertEqual(first_result.exit_code, 0)

            env = ConfigurationManager().get("test")
            self.assertIsNotNone(env)
            self.assertEqual(env.profile, self.profile_name)
            self.assertEqual(env.workspace_dir, ws_dir)

    def test_update_configuration(self, *args):
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            invoke_cli_runner(
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

            new_ws_dir = ws_dir + "/updated"
            invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "test",
                    "--profile",
                    self.profile_name,
                    "--workspace-dir",
                    new_ws_dir,
                ],
            )

            env = ConfigurationManager().get("test")
            self.assertIsNotNone(env)
            self.assertEqual(env.profile, self.profile_name)
            self.assertEqual(env.workspace_dir, new_ws_dir)


if __name__ == "__main__":
    unittest.main()
