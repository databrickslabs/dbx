import logging
import pathlib
import tempfile
import unittest
from uuid import uuid4

from click.testing import CliRunner
from cookiecutter.main import cookiecutter
from path import Path
from setuptools import sandbox

from dbx.cli.configure import configure
from dbx.cli.deploy import deploy
from dbx.cli.init import init
from dbx.cli.launch import launch

CICD_TEMPLATES_URI = "https://github.com/databrickslabs/cicd-templates.git"


def invoke_cli_runner(*args, **kwargs):
    """
    Helper method to invoke the CliRunner while asserting that the exit code is actually 0.
    """
    res = CliRunner().invoke(*args, **kwargs)
    assert res.exit_code == 0, 'Exit code was not 0. Output is: {}'.format(res.output)
    return res


JSONNET_TEMPLATES_PATH = {
    "AWS": "tests/templates/deployment-aws.jsonnet",
    "Azure": "tests/templates/deployment-azure.jsonnet"
}


# noinspection PyBroadException
class DbxLaunchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp()

    def provide_suite(self):
        deployment_template_path = JSONNET_TEMPLATES_PATH[self.cloud_type]
        deployment_template = pathlib.Path(deployment_template_path).read_text().replace("{project_name}",
                                                                                         self.project_name)

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
                pathlib.Path(".dbx/deployment.jsonnet").write_text(deployment_template)

                sandbox.run_setup('setup.py', ['-q', 'clean', 'bdist_wheel'])

                invoke_cli_runner(deploy, [
                    "--environment=test",
                    "--jobs=%s-pipeline1" % self.project_name
                ])

                logging.info("Test deployment (one job) - done")

                invoke_cli_runner(deploy, [
                    "--environment=test"
                ])

                logging.info("Test deployment - done")

                invoke_cli_runner(launch, [
                    "--environment=test",
                    '--job=%s-pipeline1' % self.project_name,
                    '--trace'
                ])

                logging.info("Test launch - done")

    def test_launch_aws(self):
        self.project_name = "dev_dbx_aws_%s" % str(uuid4()).split("-")[0]
        self.profile_name = "dbx-dev-aws"
        self.workspace_dir = "/Shared/dbx/projects/%s" % self.project_name
        self.cloud_type = "AWS"

        logging.info(
            "Initializing AWS test suite with temp dir: %s and project: %s" % (self.test_dir, self.project_name))
        self.provide_suite()

    def test_launch_azure(self):
        self.project_name = "dev_dbx_azure_%s" % str(uuid4()).split("-")[0]
        self.profile_name = "dbx-dev-azure"
        self.workspace_dir = "/dbx/projects/%s" % self.project_name
        self.cloud_type = "Azure"
        logging.info(
            "Initializing Azure test suite with temp dir: %s and project: %s" % (self.test_dir, self.project_name))
        self.provide_suite()

    def tearDown(self) -> None:
        pass


if __name__ == '__main__':
    unittest.main()
