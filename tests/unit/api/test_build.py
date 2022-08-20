from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from dbx.api.build import prepare_build
from dbx.models.deployment import BuildConfiguration


def test_empty(capsys):
    prepare_build(BuildConfiguration(no_build=True))
    res = capsys.readouterr()
    assert "No build actions will be performed" in res.out


def test_commands(mocker: MockerFixture, capsys):
    exec_mock = MagicMock()
    mocker.patch("dbx.api.build.execute_shell_command", exec_mock)
    conf = BuildConfiguration(commands=["sleep 1", "sleep 2", "sleep 3"])
    prepare_build(conf)
    res = capsys.readouterr()
    assert "Running the build commands" in res.out
    assert exec_mock.call_count == 3


def test_poetry(mocker: MockerFixture):
    exec_mock = MagicMock()
    mocker.patch("dbx.api.build.execute_shell_command", exec_mock)
    conf = BuildConfiguration(python="poetry")
    prepare_build(conf)
    exec_mock.assert_called_once_with("-m poetry build -f wheel", with_python_executable=True)


def test_flit(mocker: MockerFixture):
    exec_mock = MagicMock()
    mocker.patch("dbx.api.build.execute_shell_command", exec_mock)
    conf = BuildConfiguration(python="flit")
    prepare_build(conf)
    exec_mock.assert_called_once_with("-m flit build --format wheel", with_python_executable=True)
