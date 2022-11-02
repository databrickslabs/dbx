from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from dbx.api.launch.pipeline_models import PipelineUpdateState, UpdateStatus
from dbx.api.launch.runners.base import RunData, PipelineUpdateResponse
from dbx.api.launch.tracer import RunTracer, PipelineTracer


def test_tracer_with_interruption(mocker: MockerFixture):
    mocker.patch("dbx.api.launch.tracer.trace_run", MagicMock(side_effect=KeyboardInterrupt()))
    cancel_mock = MagicMock()
    mocker.patch("dbx.api.launch.tracer.cancel_run", cancel_mock)
    _st, _ = RunTracer.start(kill_on_sigterm=True, api_client=MagicMock(), run_data=RunData(run_id=1))
    assert _st == "CANCELLED"
    cancel_mock.assert_called_once()


@pytest.fixture
def tracer_mock():
    client = MagicMock()
    client.perform_query = MagicMock(
        side_effect=[
            {"status": UpdateStatus.ACTIVE, "latest_update": {"update_id": "a", "state": PipelineUpdateState.CREATED}},
            {
                "status": UpdateStatus.ACTIVE,
                "latest_update": {"update_id": "a", "state": PipelineUpdateState.WAITING_FOR_RESOURCES},
            },
            {"status": UpdateStatus.ACTIVE, "latest_update": {"update_id": "a", "state": PipelineUpdateState.RUNNING}},
            {"status": UpdateStatus.ACTIVE, "latest_update": {"update_id": "a", "state": PipelineUpdateState.RUNNING}},
            {
                "status": UpdateStatus.TERMINATED,
                "latest_update": {"update_id": "a", "state": PipelineUpdateState.COMPLETED},
            },
        ]
    )
    return client


def test_pipeline_tracer(tracer_mock):
    pipeline_update = PipelineUpdateResponse(update_id="a", request_id="b")
    final_state = PipelineTracer.start(api_client=tracer_mock, pipeline_id="aaa-bbb", process_info=pipeline_update)
    assert final_state == PipelineUpdateState.COMPLETED
