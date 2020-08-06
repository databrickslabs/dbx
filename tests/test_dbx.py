import os
import unittest

from click.testing import CliRunner

from dbx.cli.init import init


class DbxTest(unittest.TestCase):
    def test_init(self):
        runner = CliRunner()
        project_name = "dbx-test"
        args = [
            "--project-name", project_name,
            "--cloud", "AWS",
            "--pipeline-engine", "GitHub Actions"
        ]
        with runner.isolated_filesystem():
            result = runner.invoke(init, args)
            self.assertFalse(result.exception)
            self.assertTrue(os.path.exists(project_name))
