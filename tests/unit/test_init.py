"""
Please note that init method is heavily used in the conftest while generating the projects.
Therefore, in this class we only test negative cases and some small options
"""
from conftest import invoke_cli_runner
from dbx.commands.init import init, DEFAULT_TEMPLATE
import pytest

from dbx.constants import TEMPLATE_ROOT_PATH


def test_negative_template_and_path():
    with pytest.raises(Exception):
        result = invoke_cli_runner(init, ["--template=python_basic", "--path=some/path"], expected_error=True)
        raise result.exception


def test_positive_from_path():
    sample_path = TEMPLATE_ROOT_PATH / DEFAULT_TEMPLATE / "render"
    result = invoke_cli_runner(init, [f"--path={sample_path.absolute()}"])
    assert result.exit_code == 0
