from pathlib import Path

import pytest
import yaml

from dbx.api.build import execute_shell_command


@pytest.mark.disable_auto_execute_mock
def test_python_basic_sanity_check(temp_project):
    execute_shell_command(
        "-m prospector -s verylow -W pylint -W pep8 -W pep257 -W pydocstyle -W pycodestyle",
        with_python_executable=True,
    )
    execute_shell_command("-m black .", with_python_executable=True)
    execute_shell_command("-m black --check .", with_python_executable=True)
    yaml_files = Path(".").rglob("**/*.yml")
    for _file in yaml_files:
        yaml.safe_load(_file.read_text(encoding="utf-8"))
