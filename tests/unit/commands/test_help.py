import typer.rich_utils

from dbx.custom import _make_custom_text
from tests.unit.conftest import invoke_cli_runner

typer.rich_utils._make_rich_rext = _make_custom_text


def test_help(temp_project):
    invoke_cli_runner("deploy --help")
