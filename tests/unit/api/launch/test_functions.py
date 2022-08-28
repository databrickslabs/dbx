from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from dbx.api.launch.functions import cancel_run


def test_cancel(mocker: MockerFixture):
    wait_mock = mocker.patch("dbx.api.launch.functions.wait_run", MagicMock())
    client = MagicMock()
    cancel_run(client, {"run_id": 1})
    wait_mock.assert_called()

