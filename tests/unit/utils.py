import logging
import shutil
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4
from path import Path as ContextPath
from click.testing import CliRunner
from databricks_cli.configure.provider import DatabricksConfig

from dbx.commands.init import init

TEST_HOST = "https:/dbx.cloud.databricks.com"
TEST_TOKEN = "dapiDBXTEST"
DEFAULT_DEPLOYMENT_FILE_PATH = Path("conf/deployment.json")

test_dbx_config = DatabricksConfig.from_token(TEST_HOST, TEST_TOKEN)


def initialize_cookiecutter(project_name):
    invoke_cli_runner(
        init,
        ["-p", f"project_name={project_name}", "--no-input"],
    )


def invoke_cli_runner(*args, **kwargs):
    """
    Helper method to invoke the CliRunner while asserting that the exit code is actually 0.
    """
    expected_error = kwargs.pop("expected_error") if kwargs.get("expected_error") else None

    res = CliRunner().invoke(*args, **kwargs)

    if res.exit_code != 0:
        if not expected_error:
            logging.error("Exception in the cli runner: %s" % res.exception)
            raise res.exception
        else:
            logging.info("Expected exception in the cli runner: %s" % res.exception)

    return res


class DbxTest(unittest.TestCase):
    def setUp(self) -> None:
        self.test_dir = tempfile.mkdtemp()
        self.project_name = "dev_dbx_%s" % str(uuid4()).split("-")[0]
        self.profile_name = "dbx-test"
        logging.info("Launching test in directory %s with project name %s" % (self.test_dir, self.project_name))

        with ContextPath(self.test_dir):
            initialize_cookiecutter(self.project_name)

        self.project_dir = ContextPath(self.test_dir).joinpath(self.project_name)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir)
