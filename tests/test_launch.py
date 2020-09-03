import logging
import pathlib
import tempfile
import unittest
from uuid import uuid4

from click.testing import CliRunner
from path import Path
from setuptools import sandbox
import traceback
from dbx.cli.configure import configure
from dbx.cli.deploy import deploy
from dbx.cli.init import init
from dbx.cli.launch import launch
from .utils import initialize_cookiecutter


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
            initialize_cookiecutter(self.project_name)
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

                invoke_cli_runner(deploy, [
                    "--environment=test",
                    "--requirements=runtime_requirements.txt"
                ])

                logging.info("Test deployment (with requirements) - done")

                logging.info("Test deployment - done")

                invoke_cli_runner(launch, [
                    "--environment=test",
                    '--job=%s-pipeline1' % self.project_name,
                    '--trace'
                ])

                logging.info("Test launch (with trace) - done")

                invoke_cli_runner(launch, [
                    "--environment=test",
                    '--job=%s-pipeline1' % self.project_name,
                ])

                logging.info("Test launch (without trace) - done")

                invoke_cli_runner(launch, [
                    "--environment=test",
                    '--job=%s-pipeline1' % self.project_name,
                    "--existing-runs=wait"
                ])

                logging.info("Test launch (with wait option) - done")

                invoke_cli_runner(launch, [
                    "--environment=test",
                    '--job=%s-pipeline1' % self.project_name,
                    "--existing-runs=cancel",
                    "--trace"
                ])

                logging.info("Test launch (with cancel option) - done")

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
