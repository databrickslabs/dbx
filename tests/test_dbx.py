import logging
import tempfile
import unittest
from pathlib import Path as Pathlib
from uuid import uuid4

from click.testing import CliRunner
from cookiecutter.main import cookiecutter
from path import Path

from dbx.cli.configure import configure
from dbx.cli.deploy import deploy
from dbx.cli.init import init
from dbx.cli.launch import launch
from dbx.cli.utils import read_json, write_json

CICD_TEMPLATES_URI = "https://github.com/databrickslabs/cicd-templates.git"


def invoke_cli_runner(*args, **kwargs):
    """
    Helper method to invoke the CliRunner while asserting that the exit code is actually 0.
    """
    res = CliRunner().invoke(*args, **kwargs)
    assert res.exit_code == 0, 'Exit code was not 0. Output is: {}'.format(res.output)
    return res


def reconfigure_json_files(project_name):
    # delete schedules from all test jobs to avoid any re-launches.
    # also, add meaningful names to the jobs
    configs = list(Pathlib(".").rglob("*.json"))
    for conf in configs:
        content = read_json(str(conf))
        content.pop("schedule", None)
        content["name"] = project_name + "-test-launch"
        write_json(content, str(conf))


# noinspection PyBroadException
class DbxTest(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp()

    def provide_suite(self):
        with Path(self.test_dir):
            cookiecutter(CICD_TEMPLATES_URI, no_input=True, extra_context={"project_name": self.project_name})
            with Path(self.project_name):
                reconfigure_json_files(self.project_name)
                invoke_cli_runner(init, ["--project-name", self.project_name])
                self.assertTrue(Path(".dbx.json").exists())
                logging.info("Project initialization - done")

                invoke_cli_runner(configure, [
                    "--name", "test",
                    "--profile", self.profile_name,
                    "--workspace-dir", self.workspace_dir
                ])
                logging.info("Project configuration - done")

                invoke_cli_runner(deploy, [
                    "--environment=test",
                    "--dirs=pipelines",
                    "--files=.dbx.json",
                    "--rglobs=*.txt"
                ])
                logging.info("Test deployment - done")

                invoke_cli_runner(launch, [
                    "--environment=test",
                    '--entrypoint-file=pipelines/pipeline1/pipeline_runner.py',
                    '--job-conf-file=pipelines/pipeline1/job_spec_%s.json' % self.cloud_type,
                    '--trace'
                ])
                logging.info("Test launch - done")

    def test_aws(self):
        logging.info("Initializing AWS test suite with temp dir: %s" % self.test_dir)
        self.project_name = "dev_dbx_aws_%s" % str(uuid4()).split("-")[0]
        self.profile_name = "dbx-dev-aws"
        self.workspace_dir = "/Shared/dbx/projects/%s" % self.project_name
        self.cloud_type = "aws"
        self.provide_suite()

    def test_azure(self):
        logging.info("Initializing Azure test suite with temp dir: %s" % self.test_dir)
        self.project_name = "dev_dbx_azure_%s" % str(uuid4()).split("-")[0]
        self.profile_name = "dbx-dev-azure"
        self.workspace_dir = "/Shared/dbx/projects/%s" % self.project_name
        self.cloud_type = "azure"
        self.provide_suite()

    def tearDown(self) -> None:
        pass


if __name__ == '__main__':
    unittest.main()
