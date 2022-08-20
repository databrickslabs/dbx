from pathlib import Path
from uuid import uuid4

import yaml

from dbx.api.build import execute_shell_command
from dbx.commands.init import init
from tests.unit.conftest import in_context


def test_python_basic_sanity_check(tmp_path: Path):
    project_name = "dev_dbx_%s" % str(uuid4()).split("-")[0]
    with in_context(tmp_path):
        init(
            template="python_basic", path=None, package=None, parameters=[f"project_name={project_name}"], no_input=True
        )
        execute_shell_command("-m prospector -s verylow -W pylint -W pep8 -W pep257", with_python_executable=True)
        execute_shell_command("-m black .", with_python_executable=True)
        yaml_files = Path(".").rglob("**/*.yml")
        for _file in yaml_files:
            yaml.safe_load(_file.read_text(encoding="utf-8"))
