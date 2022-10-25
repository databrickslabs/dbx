from typing import List
from unittest.mock import MagicMock

import pytest
import yaml
from databricks_cli.sdk import ClusterService
from pytest_mock import MockerFixture

from dbx.api.adjuster.adjuster import Adjuster, AdditionalLibrariesProvider
from dbx.models.deployment import AnyWorkflow
from dbx.models.workflow.v2dot0.workflow import Workflow as V2dot0Workflow
from dbx.models.workflow.v2dot1.workflow import Workflow as V2dot1Workflow

TEST_PAYLOADS = {
    "legacy": """
    name: "a"
    some_task: "a"
    existing_cluster_name: "some-cluster"
    """,
    "property": """
    name: "a"
    tasks:
      - task_key: "a"
        some_task: "a"
        existing_cluster_id: "cluster://some-cluster"
    """,
    "duplicated": """
    name: "a"
    tasks:
      - task_key: "a"
        some_task: "a"
        existing_cluster_id: "cluster://some-duplicated-cluster"
    """,
    "not_found": """
    name: "a"
    tasks:
      - task_key: "a"
        some_task: "a"
        existing_cluster_id: "cluster://some-not-found-cluster"
    """,
}


@pytest.fixture
def existing_cluster_mock(mocker: MockerFixture):
    mocker.patch.object(
        ClusterService,
        "list_clusters",
        MagicMock(
            return_value={
                "clusters": [
                    {"cluster_name": "some-cluster", "cluster_id": "some-id"},
                    {"cluster_name": "some-duplicated-cluster", "cluster_id": "some-id-1"},
                    {"cluster_name": "some-duplicated-cluster", "cluster_id": "some-id-2"},
                ]
            }
        ),
    )


def convert_to_workflow_with_legacy_support(key: str, payload: str) -> List[AnyWorkflow]:
    _base_class = V2dot0Workflow if key == "legacy" else V2dot1Workflow
    return [_base_class(**yaml.safe_load(payload))]


@pytest.mark.parametrize("key", list(TEST_PAYLOADS.keys()))
def test_instance_pools(key, existing_cluster_mock):
    _wf = convert_to_workflow_with_legacy_support(key, TEST_PAYLOADS[key])
    _adj = Adjuster(
        api_client=MagicMock(),
        additional_libraries=AdditionalLibrariesProvider(core_package=None),
        file_uploader=MagicMock(),
    )
    if key in ["duplicated", "not_found"]:
        with pytest.raises(AssertionError):
            _adj.traverse(_wf)
    elif key == "legacy":
        _adj.traverse(_wf)
        assert _wf[0].existing_cluster_id == "some-id"
    else:
        _adj.traverse(_wf)
        assert _wf[0].get_task("a").existing_cluster_id == "some-id"
