import logging
import shutil
import tempfile
import unittest
from uuid import uuid4

from click.testing import CliRunner
from cookiecutter.main import cookiecutter
from databricks_cli.configure.provider import DatabricksConfig
from path import Path
from retry import retry

CICD_TEMPLATES_URI = "https://github.com/databrickslabs/cicd-templates.git"
TEST_HOST = "https:/dbx.cloud.databricks.com"
TEST_TOKEN = "dapiDBXTEST"

test_dbx_config = DatabricksConfig.from_token(TEST_HOST, TEST_TOKEN)


@retry(tries=10, delay=5, backoff=5)
def initialize_cookiecutter(project_name):
    cookiecutter(CICD_TEMPLATES_URI, no_input=True, extra_context={"project_name": project_name})


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
        logging.info(
            "Launching configure test in directory %s with project name %s" % (self.test_dir, self.project_name)
        )

        with Path(self.test_dir):
            initialize_cookiecutter(self.project_name)

        self.project_dir = Path(self.test_dir).joinpath(self.project_name)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir)
