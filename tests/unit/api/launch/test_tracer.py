from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from dbx.api.launch.tracer import RunTracer


def test_tracer_with_interruption(mocker: MockerFixture):
    mocker.patch("dbx.api.launch.tracer.trace_run", MagicMock(side_effect=KeyboardInterrupt()))
    cancel_mock = MagicMock()
    mocker.patch("dbx.api.launch.tracer.cancel_run", cancel_mock)
    _st, _ = RunTracer.start(kill_on_sigterm=True, api_client=MagicMock(), run_data={"run_id": 1})
    assert _st == "CANCELLED"
    cancel_mock.assert_called_once()
