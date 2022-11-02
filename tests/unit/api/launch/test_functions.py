from unittest.mock import MagicMock

import mlflow
from pytest_mock import MockerFixture

from dbx.api.launch.functions import cancel_run, wait_run
from dbx.api.launch.runners.base import RunData
from dbx.api.storage.io import StorageIO
from dbx.utils.json import JsonUtils


def test_cancel(mocker: MockerFixture):
    wait_mock = mocker.patch("dbx.api.launch.functions.wait_run", MagicMock())
    client = MagicMock()
    cancel_run(client, RunData(run_id=1))
    wait_mock.assert_called()


def test_wait_run(mocker: MockerFixture):
    mocker.patch(
        "dbx.api.launch.functions.get_run_status",
        MagicMock(
            side_effect=[
                {"state": {"life_cycle_state": "RUNNING"}},
                {"state": {"life_cycle_state": "TERMINATED"}},
            ]
        ),
    )
    client = MagicMock()
    wait_run(client, RunData(**{"run_id": 1}))


def test_load_file(tmp_path):
    content = {"a": 1}
    with mlflow.start_run() as test_run:
        _file = tmp_path / "conf.json"
        JsonUtils.write(_file, content)
        mlflow.log_artifact(str(_file.absolute()), ".dbx")
        _result = StorageIO.load(test_run.info.run_id, "conf.json")
        assert _result == content
