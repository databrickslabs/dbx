import os
import unittest

from click.testing import CliRunner

from databrickslabs_cicdtemplates.cli.init import init


class DbxTest(unittest.TestCase):
    def test_init(self):
        runner = CliRunner()
        project_name = "dbx-test"
        args = [
            "--project-name", project_name,
            "--author", "dev",
            "--cloud", "AWS",
            "--cicd_tool", "GitHub Actions"
        ]
        with runner.isolated_filesystem():
            result = runner.invoke(init, args)
            self.assertFalse(result.exception)
            self.assertFalse(os.path.exists(project_name))
