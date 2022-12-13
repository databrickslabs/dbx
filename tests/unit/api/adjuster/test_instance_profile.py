from typing import List
from unittest.mock import MagicMock

import pytest
import yaml

from dbx.api.adjuster.adjuster import Adjuster, AdditionalLibrariesProvider
from dbx.models.workflow.v2dot1.workflow import Workflow

TEST_PAYLOADS = {
    "legacy": """
    job_clusters:
      - job_cluster_key: "some-cluster"
        new_cluster:
          spark_version: "some"
          aws_attributes:
            instance_profile_name: "some-instance-profile"
    name: "instance-profile-test"
    some_task: "a"
    """,
    "property": """
    job_clusters:
      - job_cluster_key: "some-cluster"
        new_cluster:
          spark_version: "some"
          aws_attributes:
            instance_profile_arn: "instance-profile://some-instance-profile"
    name: "instance-profile-test"
    some_task: "a"
    """,
    "duplicated": """
    job_clusters:
      - job_cluster_key: "some-cluster"
        new_cluster:
          spark_version: "some"
          aws_attributes:
            instance_profile_arn: "instance-profile://some-duplicated-profile"
    name: "instance-profile-test"
    some_task: "a"
    """,
    "not_found": """
    job_clusters:
      - job_cluster_key: "some-cluster"
        new_cluster:
          spark_version: "some"
          aws_attributes:
            instance_profile_arn: "instance-profile://some-not-found-profile"
    name: "instance-profile-test"
    some_task: "a"
    """,
}


def convert_to_workflow(payload: str) -> List[Workflow]:
    return [Workflow(**yaml.safe_load(payload))]


@pytest.fixture
def instance_profile_mock() -> MagicMock:
    client = MagicMock()
    client.perform_query = MagicMock(
        return_value={
            "instance_profiles": [
                {"instance_profile_arn": "some-arn/some-instance-profile"},
                {"instance_profile_arn": "some-arn/some-duplicated-profile"},
                {"instance_profile_arn": "some-arn/some-duplicated-profile"},
            ]
        }
    )
    return client


@pytest.mark.parametrize("key", list(TEST_PAYLOADS.keys()))
def test_instance_profile(key, instance_profile_mock):
    _wf = convert_to_workflow(TEST_PAYLOADS[key])
    _adj = Adjuster(
        api_client=instance_profile_mock,
        additional_libraries=AdditionalLibrariesProvider(core_package=None),
        file_uploader=MagicMock(),
    )
    if key in ["duplicated", "not_found"]:
        with pytest.raises(AssertionError):
            _adj.traverse(_wf)
    else:
        _adj.traverse(_wf)
        assert (
            _wf[0].get_job_cluster_definition("some-cluster").new_cluster.aws_attributes.instance_profile_arn
            == "some-arn/some-instance-profile"
        )
