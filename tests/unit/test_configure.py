import unittest

from dbx.commands.configure import configure
from dbx.utils.common import InfoFile
from .utils import invoke_cli_runner, DbxTest

"""
What do we test: dbx configure
Expected behaviour: if no .dbx folder provided -> create folder, initialize InfoFile, create environment
"""


class ConfigureTest(DbxTest):

    def test_configure(self, *args) -> None:
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            first_result = invoke_cli_runner(configure, [
                "--environment", "test",
                "--profile", self.profile_name,
                "--workspace-dir", ws_dir
            ])

            self.assertEqual(first_result.exit_code, 0)

            env = InfoFile.get("environments").get("test")
            self.assertIsNotNone(env)
            self.assertEqual(env["profile"], self.profile_name)
            self.assertEqual(env["workspace_dir"], ws_dir)


if __name__ == '__main__':
    unittest.main()
