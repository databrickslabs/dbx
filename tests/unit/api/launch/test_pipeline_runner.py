import pytest

from dbx.api.launch.runners.base import PipelineUpdateResponse
from dbx.api.launch.runners.pipeline import PipelineLauncher, PipelinesRunPayload

TEST_PIPELINE_ID = "aaa-bbb"
TEST_PIPELINE_UPDATE_PAYLOAD = {"update_id": "u1", "request_id": "r1"}


def test_basic(pipeline_launch_mock):
    launcher = PipelineLauncher("some", api_client=pipeline_launch_mock)
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
def test_with_parameters(payload, expected, pipeline_launch_mock):
    launcher = PipelineLauncher("some", api_client=pipeline_launch_mock, parameters=payload)
    assert launcher.parameters is not None
    assert launcher.parameters == expected
