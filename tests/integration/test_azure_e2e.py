import tempfile
import unittest
from uuid import uuid4
from click.testing import CliRunner
from cookiecutter.main import cookiecutter
from path import Path
import shutil
from dbx.commands.configure import configure
from dbx.commands.deploy import deploy
from dbx.commands.launch import launch
import logging


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


class AzureE2ETest(unittest.TestCase):
    def setUp(self) -> None:
        self._project_name = f"dbx_dev_azure_{str(uuid4()).split('-')[0]}"
        self._temp_dir = tempfile.mkdtemp()
        cookiecutter("https://github.com/databrickslabs/cicd-templates.git",
                     checkout="dbx",
                     output_dir=self._temp_dir,
                     extra_context={
                         "cloud": "Azure",
                         "project_name": self._project_name,
                         "environment": "dbx-dev-azure"
                     },
                     no_input=True)

    def test_e2e(self):
        project_path = Path(self._temp_dir).joinpath(self._project_name)
        logging.info(f"Local project directory: {project_path}")
        with project_path:
            configure_result = invoke_cli_runner(configure, [
                "-e", "dbx-dev-azure",
                "--profile", "dbx-dev-azure",
                "--workspace-dir", f"/dbx/projects/{self._project_name}"
            ])

            self.assertEqual(configure_result.exit_code, 0)
            logging.info(configure_result.output)
            deploy_result = invoke_cli_runner(deploy, [
                "-e", "dbx-dev-azure",
                "--requirements-file", "unit-requirements.txt"
            ])

            self.assertEqual(deploy_result.exit_code, 0)
            logging.info(deploy_result.output)

            logging.info("Launching job ib the e2e test")
            launch_result = invoke_cli_runner(launch, [
                "-e", "dbx-dev-azure",
                "--job", f"{self._project_name}-sample",
                "--trace"
            ])

            self.assertEqual(launch_result.exit_code, 0)
            logging.info(launch_result.output)

    def tearDown(self) -> None:
        try:
            shutil.rmtree(self._temp_dir)
        except:
            pass


if __name__ == '__main__':
    unittest.main()
