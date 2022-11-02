from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from dbx.api.launch.pipeline_models import PipelineGlobalState
from dbx.api.launch.runners.base import PipelineUpdateResponse
from dbx.api.launch.runners.pipeline import PipelineLauncher, PipelinesRunPayload

TEST_PIPELINE_ID = "aaa-bbb"
TEST_PIPELINE_UPDATE_PAYLOAD = {"update_id": "u1", "request_id": "r1"}


@pytest.fixture
def launch_mock(mocker: MockerFixture):
    client = MagicMock()
    client.perform_query = MagicMock(
        side_effect=[
            {"statuses": [{"pipeline_id": TEST_PIPELINE_ID, "name": "some"}]},  # get pipeline
            {"state": PipelineGlobalState.RUNNING},  # get current state
            {},  # stop pipeline
            {"state": PipelineGlobalState.IDLE},  # second verification get
            TEST_PIPELINE_UPDATE_PAYLOAD,  # start pipeline
        ]
    )
    return client


def test_basic(launch_mock):
    launcher = PipelineLauncher("some", api_client=launch_mock)
    process_info, object_id = launcher.launch()
    assert object_id == TEST_PIPELINE_ID
    assert process_info == PipelineUpdateResponse(**TEST_PIPELINE_UPDATE_PAYLOAD)


@pytest.mark.parametrize(
    "payload, expected",
    [
        ('{"full_refresh": "true"}', PipelinesRunPayload(full_refresh=True)),
        ('{"refresh_selection": ["tab1"]}', PipelinesRunPayload(refresh_selection=["tab1"])),
        (
            '{"refresh_selection": ["tab1"], "full_refresh_selection": ["tab2"]}',
            PipelinesRunPayload(refresh_selection=["tab1"], full_refresh_selection=["tab2"]),
        ),
    ],
)
def test_with_parameters(payload, expected, launch_mock):
    launcher = PipelineLauncher("some", api_client=launch_mock, parameters=payload)
    assert launcher.parameters is not None
    assert launcher.parameters == expected
