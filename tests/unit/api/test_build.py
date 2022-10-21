import inspect
from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from dbx.models.build import BuildConfiguration


def test_empty(capsys):
    BuildConfiguration(no_build=True).trigger_build_process()
    res = capsys.readouterr()
    assert "No build actions will be performed" in res.out


def test_no_action(capsys):
    BuildConfiguration(no_build=False, commands=None, python=None).trigger_build_process()
    res = capsys.readouterr()
    assert "skipping the build stage" in res.out


def test_commands(temp_project, mocker: MockerFixture, capsys):
    exec_mock = MagicMock()
    mocker.patch("dbx.models.build.execute_shell_command", exec_mock)
    conf = BuildConfiguration(commands=["sleep 1", "sleep 2", "sleep 3"])
    conf.trigger_build_process()
    res = capsys.readouterr()
    assert "Running the build commands" in res.out
    assert exec_mock.call_count == 3


def test_poetry(temp_project):
    (temp_project / "setup.py").unlink()
    (temp_project / "pyproject.toml").unlink()

    pyproject_content = f"""
    [tool.poetry]
    name = "{temp_project.name}"
    version = "0.1.0"
    description = "some description"
    authors = []

    [tool.poetry.dependencies]
    python = "^3.8"

    [build-system]
    requires = ["poetry-core"]
    build-backend = "poetry.core.masonry.api"
    """

    (temp_project / "pyproject.toml").write_text(inspect.cleandoc(pyproject_content))
    conf = BuildConfiguration(python="poetry")
    conf.trigger_build_process()


def test_flit(temp_project, mocker: MockerFixture):
    exec_mock = MagicMock()
    mocker.patch("dbx.models.build.execute_shell_command", exec_mock)
    conf = BuildConfiguration(python="flit")
    conf.trigger_build_process()
    exec_mock.assert_called_once_with(cmd="-m flit build --format wheel", with_python_executable=True)
