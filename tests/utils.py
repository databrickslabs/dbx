import logging
import traceback

from click.testing import CliRunner
from cookiecutter.main import cookiecutter
from retry import retry

CICD_TEMPLATES_URI = "https://github.com/databrickslabs/cicd-templates.git"


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
            traceback_object = res.exc_info[2]
            traceback.print_tb(traceback_object)
        else:
            logging.info("Expected exception in the cli runner: %s" % res.exception)

    return res
