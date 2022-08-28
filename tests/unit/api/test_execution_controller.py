from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from dbx.api.context import RichExecutionContextClient, LowLevelExecutionContextClient


def test_execution_controller(mocker: MockerFixture, temp_project):
    exec_mock = mocker.patch.object(LowLevelExecutionContextClient, "execute_command", MagicMock())
    client = RichExecutionContextClient(v2_client=MagicMock(), cluster_id="a")
    client.install_package("/some/whatever/file", "some-extras")
    exec_mock.assert_called_once_with(
        f'%pip install --force-reinstall "/some/whatever/file[some-extras]"', verbose=False
    )
