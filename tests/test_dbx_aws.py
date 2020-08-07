import os
import tempfile
import unittest

from click.testing import CliRunner, Result
from databricks_cli.clusters.api import ClusterService
from databricks_cli.sdk.api_client import ApiClient
from dotenv import load_dotenv
from path import Path

from dbx.cli.clusters import create_dev_cluster
from dbx.cli.execute import execute
from dbx.cli.init import init
from dbx.cli.utils import read_json

if not os.environ.get("DATABRICKS_HOST"):
    # for local runs
    load_dotenv(".env.aws")


class DbxAwsTest(unittest.TestCase):
    test_project_name = "dbx_test_aws"

    @staticmethod
    def assert_runner(result: Result):
        if result.exception:
            print(result.output)
            raise result.exception

    @classmethod
    def setUpClass(cls) -> None:
        cls.test_dir = tempfile.mkdtemp()
        cls.runner = CliRunner()

    def test_process(self):
        args = [
            "--project-name", self.test_project_name,
            "--cloud", "AWS",
            "--pipeline-engine", "GitHub Actions"
        ]
        with Path(self.test_dir):
            result = self.runner.invoke(init, args)
            self.assert_runner(result)
            self.assertTrue(os.path.exists(self.test_project_name))

            with Path(os.path.join(self.test_dir, self.test_project_name)):
                cluster_creation = self.runner.invoke(create_dev_cluster)
                self.assert_runner(cluster_creation)

                job_execution = self.runner.invoke(execute, ["--job-name", "batch"])
                self.assert_runner(job_execution)

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            with Path(os.path.join(cls.test_dir, cls.test_project_name)):
                cluster_id = read_json(".dbx.lock.json")["dev_cluster_id"]
                api_client = ApiClient(token=os.environ["DATABRICKS_TOKEN"], host=os.environ["DATABRICKS_HOST"])
                cluster_service = ClusterService(api_client)
                cluster_service.permanent_delete_cluster(cluster_id)
        except Exception as e:
            print("Test class teardown failed due to error: %s" % e)


if __name__ == '__main__':
    unittest.main()
