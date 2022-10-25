from unittest.mock import MagicMock

import pytest
from databricks_cli.sdk import InstancePoolService
from pytest_mock import MockerFixture

from dbx.api.adjuster.adjuster import Adjuster, AdditionalLibrariesProvider
from .test_instance_profile import convert_to_workflow

TEST_PAYLOADS = {
    "legacy": """
    job_clusters:
      - job_cluster_key: "some-cluster"
        new_cluster:
          spark_version: "some"
          instance_pool_name: "some-pool"
          driver_instance_pool_name: "some-pool"
    name: "test"
    some_task: "a"
    """,
    "property": """
    job_clusters:
      - job_cluster_key: "some-cluster"
        new_cluster:
          spark_version: "some"
          instance_pool_id: "instance-pool://some-pool"
          driver_instance_pool_id: "instance-pool://some-pool"
    name: "test"
    some_task: "a"
    """,
    "duplicated": """
    job_clusters:
      - job_cluster_key: "some-cluster"
        new_cluster:
          spark_version: "some"
          instance_pool_id: "instance-pool://some-duplicated-pool"
    name: "instance-profile-test"
    some_task: "a"
    """,
    "not_found": """
    job_clusters:
      - job_cluster_key: "some-cluster"
        new_cluster:
          spark_version: "some"
          aws_attributes:
            instance_pool_id: "instance-pool://some-non-existent-pool"
    name: "test"
    some_task: "a"
    """,
}


@pytest.fixture
def instance_pool_mock(mocker: MockerFixture):
    mocker.patch.object(
        InstancePoolService,
        "list_instance_pools",
        MagicMock(
            return_value={
                "instance_pools": [
                    {"instance_pool_name": "some-pool", "instance_pool_id": "some-id"},
                    {"instance_pool_name": "some-duplicated-pool", "instance_pool_id": "some-id-1"},
                    {"instance_pool_name": "some-duplicated-pool", "instance_pool_id": "some-id-2"},
                ]
            }
        ),
    )


@pytest.mark.parametrize("key", list(TEST_PAYLOADS.keys()))
def test_instance_pools(key, instance_pool_mock):
    _wf = convert_to_workflow(TEST_PAYLOADS[key])
    _adj = Adjuster(
        api_client=MagicMock(),
        additional_libraries=AdditionalLibrariesProvider(core_package=None),
        file_uploader=MagicMock(),
    )
    if key in ["duplicated", "not_found"]:
        with pytest.raises(AssertionError):
            _adj.traverse(_wf)
    else:
        _adj.traverse(_wf)
        assert _wf[0].get_job_cluster_definition("some-cluster").new_cluster.instance_pool_id == "some-id"
