import json
import logging
import pathlib
import tempfile
import traceback
import unittest
from uuid import uuid4

import time
from click.testing import CliRunner
from cookiecutter.main import cookiecutter
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.configure.config import ProfileConfigProvider
from databricks_cli.sdk.api_client import ApiClient
from path import Path
from setuptools import sandbox

from dbx.cli.configure import configure
from dbx.cli.execute import execute
from dbx.cli.init import init

CICD_TEMPLATES_URI = "https://github.com/databrickslabs/cicd-templates.git"

INTERACTIVE_CLUSTER_TEMPLATES_PATH = {
    "AWS": "tests/templates/interactive-aws.json",
    "Azure": "tests/templates/interactive-azure.json"
}
CONDA_ENV_TEMPLATE_PATH = "tests/templates/conda-env.yml"


def invoke_cli_runner(*args, **kwargs):
    """
    Helper method to invoke the CliRunner while asserting that the exit code is actually 0.
    """
    res = CliRunner().invoke(*args, **kwargs)

    if res.exit_code != 0:
        logging.error("Exception in the cli runner: %s" % res.exception)
        traceback_object = res.exc_info[2]
        traceback.print_tb(traceback_object)

    assert res.exit_code == 0, 'Exit code was not 0. Output is: {}'.format(res.output)
    return res


# noinspection PyBroadException
class DbxExecuteTest(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp()

    def provide_suite(self):
        provider = ProfileConfigProvider(self.profile_name)
        config = provider.get_config()
        api_client = ApiClient(host=config.host, token=config.token)
        template_path = INTERACTIVE_CLUSTER_TEMPLATES_PATH[self.cloud_type]
        raw_template = pathlib.Path(template_path).read_text().replace("{project_name}", self.project_name)
        template = json.loads(raw_template)
        self.cluster_api = ClusterApi(api_client)
        self.cluster_id = self.cluster_api.create_cluster(template)["cluster_id"]
        logging.info("Created a new cluster for test with id: %s" % self.cluster_id)
        time.sleep(10)  # cluster creation is an eventually-consistent operation, better to wait a bit
        conda_env_data = pathlib.Path(CONDA_ENV_TEMPLATE_PATH).read_text()

        with Path(self.test_dir):
            cookiecutter(CICD_TEMPLATES_URI, no_input=True, extra_context={"project_name": self.project_name})
            with Path(self.project_name):
                invoke_cli_runner(init, ["--project-name", self.project_name])
                self.assertTrue(Path(".dbx").exists())
                logging.info("Project initialization - done")

                invoke_cli_runner(configure, [
                    "--name", "test",
                    "--profile", self.profile_name,
                    "--workspace-dir", self.workspace_dir
                ])

                logging.info("Project configuration - done")

                sandbox.run_setup('setup.py', ['-q', 'clean', 'bdist_wheel'])

                invoke_cli_runner(execute, [
                    "--environment", "test",
                    "--cluster-name", "%s-interactive" % self.project_name,
                    "--source-file", "pipelines/pipeline1/pipeline_runner.py",
                    "--package", "dist/{project_name}-0.1.0-py3-none-any.whl".format(project_name=self.project_name)
                ])
                logging.info("Execution with package option - done")

                invoke_cli_runner(execute, [
                    "--environment", "test",
                    "--cluster-id", self.cluster_id,
                    "--source-file", "pipelines/pipeline1/pipeline_runner.py",
                    "--requirements", "runtime_requirements.txt"
                ])
                logging.info("Execution with requirements option - done")

                pathlib.Path("conda-env.yml").write_text(conda_env_data)

                invoke_cli_runner(execute, [
                    "--environment", "test",
                    "--cluster-id", self.cluster_id,
                    "--source-file", "pipelines/pipeline1/pipeline_runner.py",
                    "--conda-environment", "conda-env.yml"
                ])

                logging.info("Execution with conda-environment option - done")

                logging.info("Test launch - done")

    def test_execute_aws(self):
        self.project_name = "dev_dbx_aws_%s" % str(uuid4()).split("-")[0]
        self.profile_name = "dbx-dev-aws"
        self.workspace_dir = "/Shared/dbx/projects/%s" % self.project_name
        self.cloud_type = "AWS"

        logging.info(
            "Initializing AWS test suite with temp dir: %s and project: %s" % (self.test_dir, self.project_name))
        self.provide_suite()

    def test_execute_azure(self):
        self.project_name = "dev_dbx_azure_%s" % str(uuid4()).split("-")[0]
        self.profile_name = "dbx-dev-azure"
        self.workspace_dir = "/dbx/projects/%s" % self.project_name
        self.cloud_type = "Azure"
        logging.info(
            "Initializing Azure test suite with temp dir: %s and project: %s" % (self.test_dir, self.project_name))
        self.provide_suite()

    def tearDown(self) -> None:
        try:
            self.cluster_api.delete_cluster(self.cluster_id)
            self.cluster_api.permanent_delete(self.cluster_id)
            logging.info("Successfully deleted test cluster with id: %s" % self.cluster_id)
        except Exception as e:
            logging.error("Couldn't delete interactive cluster from the test due to: %s" % e)


if __name__ == '__main__':
    unittest.main()
