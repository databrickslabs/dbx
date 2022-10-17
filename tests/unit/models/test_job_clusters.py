import pytest
from pydantic import ValidationError

from dbx.models.workflow.v2dot1.job_cluster import JobClustersMixin


def test_empty():
    jc = JobClustersMixin(**{})
    assert jc.job_clusters == []


def test_duplicates():
    with pytest.raises(ValueError):
        JobClustersMixin(
            **{
                "job_clusters": [
                    {"job_cluster_key": "some", "new_cluster": {}},
                    {"job_cluster_key": "some", "new_cluster": {}},
                ]
            }
        )


def test_incorrect_format():
    with pytest.raises(ValidationError):
        JobClustersMixin(
            **{
                "job_clusters": {"job_cluster_key": "some", "new_cluster": {}},
            }
        )


def test_not_found():
    jc = JobClustersMixin(
        **{
            "job_clusters": [
                {"job_cluster_key": "some", "new_cluster": {"spark_version": "some"}},
            ]
        }
    )
    with pytest.raises(ValueError):
        jc.get_job_cluster_definition("non-existent")


def test_positive():
    nc_content = {"node_type_id": "some-node-type-id", "spark_version": "some"}
    jc = JobClustersMixin(
        **{
            "job_clusters": [
                {"job_cluster_key": "some", "new_cluster": nc_content},
            ]
        }
    )
    assert jc.get_job_cluster_definition("some") is not None
    assert jc.get_job_cluster_definition("some").job_cluster_key == "some"
    assert jc.get_job_cluster_definition("some").new_cluster.dict(exclude_none=True) == nc_content
