import logging
import os
import tempfile
import unittest
from uuid import uuid4

from click.testing import CliRunner
from databricks_cli.clusters.api import ClusterService
from databricks_cli.configure.config import ProfileConfigProvider
from databricks_cli.sdk.api_client import ApiClient
from path import Path

from dbx.cli.clusters import create_dev_cluster
from dbx.cli.execute import execute
from dbx.cli.init import init
from dbx.cli.utils import read_json

FORMAT = u'[%(asctime)s] %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)


class DbxTest(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp()
        self.runner = CliRunner()

    def provide_suite(self, init_args):
        with Path(self.test_dir):
            result = self.runner.invoke(init, init_args)
            self.assertFalse(result.exception)
            self.assertTrue(os.path.exists(self.project_name))

            with Path(os.path.join(self.test_dir, self.project_name)):
                cluster_creation = self.runner.invoke(create_dev_cluster, ["--profile", self.profile_name])
                self.assertFalse(cluster_creation.exception)

                job_execution = self.runner.invoke(execute, ["--job-name", "batch", "--profile", self.profile_name])
                self.assertFalse(job_execution.exception)

    def test_aws(self):
        logging.info("Initializing AWS test suite")
        self.project_name = "dev_dbx_aws_%s" % str(uuid4()).split("-")[0]
        self.profile_name = "dbx-dev-aws"
        init_args = [
            "--project-name", self.project_name,
            "--cloud", "AWS",
            "--pipeline-engine", "GitHub Actions",
            "--profile", self.profile_name,
            "--dbx-workspace-dir", "/Shared/dbx/projects",
            "--dbx-artifact-location", "dbfs:/tmp/dbx/projects"
        ]
        self.provide_suite(init_args)

    def test_azure(self):
        logging.info("Initializing Azure test suite")
        self.project_name = "dev_dbx_aws_%s" % str(uuid4()).split("-")[0]
        self.profile_name = "dbx-dev-azure"
        init_args = [
            "--project-name", self.project_name,
            "--cloud", "Azure",
            "--pipeline-engine", "GitHub Actions",
            "--profile", self.profile_name
        ]
        self.provide_suite(init_args)

    def tearDown(self) -> None:
        msg = """
            Test tear down info:
                Project name:   %s
                Profile name:   %s
                Temp directory: %s
        """ % (self.project_name, self.profile_name, self.test_dir)
        logging.info(msg)
        try:
            with Path(os.path.join(self.test_dir, self.project_name)):
                cluster_id = read_json(".dbx.lock.json")["dev_cluster_id"]
                config = ProfileConfigProvider(self.profile_name).get_config()
                api_client = ApiClient(token=config.token, host=config.host)
                cluster_service = ClusterService(api_client)
                cluster_service.permanent_delete_cluster(cluster_id)
        except:
            pass


if __name__ == '__main__':
    unittest.main()
