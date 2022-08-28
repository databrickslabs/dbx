from unittest.mock import MagicMock

import mlflow
from pytest_mock import MockerFixture

from dbx.api.launch.functions import cancel_run, load_dbx_file
from dbx.utils.json import JsonUtils


def test_cancel(mocker: MockerFixture):
    wait_mock = mocker.patch("dbx.api.launch.functions.wait_run", MagicMock())
    client = MagicMock()
    cancel_run(client, {"run_id": 1})
    wait_mock.assert_called()


def test_load_file(tmp_path):
    content = {"a": 1}
    with mlflow.start_run() as test_run:
        _file = tmp_path / "conf.json"
        JsonUtils.write(_file, content)
        mlflow.log_artifact(str(_file.absolute()), ".dbx")
        _result = load_dbx_file(test_run.info.run_id, "conf.json")
        assert _result == content
