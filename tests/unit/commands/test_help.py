import typer.rich_utils

from dbx.custom import _get_custom_help_text
from tests.unit.conftest import invoke_cli_runner

typer.rich_utils._get_help_text = _get_custom_help_text


def test_help(temp_project):
    # verify that markdown output has been properly formatted
    res = invoke_cli_runner("deploy --help")
    assert "─────────────────" in res.stdout
