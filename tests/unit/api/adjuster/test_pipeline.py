from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from dbx.api.adjuster.adjuster import Adjuster, AdditionalLibrariesProvider
from .test_instance_profile import convert_to_workflow

TEST_PAYLOADS = {
    "property": """
    name: "test"
    tasks:
      - task_key: "p1"
        pipeline_task:
          pipeline_id: "pipeline://some-pipeline"
    """,
    "duplicated": """
    name: "test"
    tasks:
      - task_key: "p1"
        pipeline_task:
          pipeline_id: "pipeline://some-duplicated-pipeline"
    """,
    "not_found": """
    name: "test"
    tasks:
      - task_key: "p1"
        pipeline_task:
          pipeline_id: "pipeline://some-non-existent-pipeline"
    """,
}


@pytest.fixture
def pipelines_mock(mocker: MockerFixture):
    client = MagicMock()
    client.perform_query = MagicMock(
        return_value={
            "statuses": [
                {"pipeline_id": "some-id", "name": "some-pipeline"},
                {"pipeline_id": "some-id-1", "name": "some-duplicated-pipeline"},
                {"pipeline_id": "some-id-2", "name": "some-duplicated-pipeline"},
            ]
        }
    )
    return client


@pytest.mark.parametrize("key", list(TEST_PAYLOADS.keys()))
def test_pipelines(key, pipelines_mock):
    _wf = convert_to_workflow(TEST_PAYLOADS[key])
    _adj = Adjuster(
        api_client=pipelines_mock,
        additional_libraries=AdditionalLibrariesProvider(core_package=None),
        file_uploader=MagicMock(),
    )
    if key in ["duplicated", "not_found"]:
        with pytest.raises(AssertionError):
            _adj.traverse(_wf)
    else:
        _adj.traverse(_wf)
        assert _wf[0].get_task("p1").pipeline_task.pipeline_id == "some-id"
