import os
import unittest

from click.testing import CliRunner
from dotenv import load_dotenv

from dbx.cli.init import init

# fixes the security if launch is not inside pipeline
if not os.environ.get("AZURE_DATABRICKS_TOKEN"):
    load_dotenv()


class DbxAzureTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        pass

    def test_init(self):
        runner = CliRunner()
        project_name = "dbx-test"
        args = [
            "--project-name", project_name,
            "--cloud", "Azure",
            "--pipeline-engine", "GitHub Actions"
        ]
        with runner.isolated_filesystem():
            result = runner.invoke(init, args)
            self.assertFalse(result.exception)
            self.assertTrue(os.path.exists(project_name))

    @classmethod
    def tearDownClass(cls) -> None:
        pass


if __name__ == '__main__':
    unittest.main()
