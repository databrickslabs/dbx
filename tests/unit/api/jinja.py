from pathlib import Path
from textwrap import dedent
from typing import Tuple

import pytest
import yaml

from dbx.api.config_reader import Jinja2ConfigReader
from dbx.api.jinja import get_last_modified_file
from dbx.constants import CUSTOM_JINJA_FUNCTIONS_PATH


@pytest.fixture()
def temp_with_file(tmp_path) -> Tuple[Path, Path]:
    _content_path = tmp_path / "content"
    _content_path.mkdir(exist_ok=True)

    (_content_path / "file1.dat").write_bytes(b"a")
    last = _content_path / "file2.dat"
    last.write_bytes(b"b")
    (_content_path / "file2.ndat").write_bytes(b"b")

    return _content_path, last


def test_latest_file_func(temp_with_file):
    latest = get_last_modified_file(str(temp_with_file[0]), "dat")
    assert str(temp_with_file[1]) == latest

    assert get_last_modified_file(str(temp_with_file[0]), "nbat") is None


def test_jinja_functions(temp_with_file):
    # quadruple quotes here because of f-string
    _file = f"""
    latest_path: {{{{ dbx.get_latest_file_from_path('{temp_with_file[0]}', "dat") }}}}
    """
    _sample_file = temp_with_file[0].parent / "sample.yml"
    _sample_file.write_text(_file)
    result = Jinja2ConfigReader._render_content(_sample_file, {})
    _content = yaml.load(result, yaml.SafeLoader)
    assert _content["latest_path"] == str(temp_with_file[1])


def test_nested_vars_behaviour(temp_with_file):
    expected_value = 1
    _file = f"""
    test: {{{{ var["l1"]["l2"] }}}}
    """
    _sample_file = temp_with_file[0].parent / "sample.yml"
    _sample_file.write_text(_file)
    result = Jinja2ConfigReader._render_content(_sample_file, {"l1": {"l2": expected_value}})
    _content = yaml.load(result, yaml.SafeLoader)
    assert _content["test"] == expected_value


def test_custom_functions(temp_project):
    _custom_func_code = """ \
    def multiply_by_two(x: int) -> int:
        return x * 2
    """
    sample_file = Path("sample.yml")
    sample_file.write_text(
        """
    test: {{ custom.multiply_by_two(1) }}
    """
    )

    CUSTOM_JINJA_FUNCTIONS_PATH.write_text(dedent(_custom_func_code), encoding="utf-8")
    result = Jinja2ConfigReader._render_content(sample_file, {})
    assert yaml.safe_load(result)["test"] == 2
