import pytest
from pydantic import ValidationError

from dbx.models.job_clusters import JobClusters


def test_empty():
    jc = JobClusters(**{})
    assert jc.job_clusters == []


def test_duplicates():
    with pytest.raises(ValueError):
        JobClusters(
            **{
                "job_clusters": [
                    {"job_cluster_key": "some", "new_cluster": {}},
                    {"job_cluster_key": "some", "new_cluster": {}},
                ]
            }
        )


def test_incorrect_format():
    with pytest.raises(ValidationError):
        JobClusters(
            **{
                "job_clusters": {"job_cluster_key": "some", "new_cluster": {}},
            }
        )


def test_not_found():
    jc = JobClusters(
        **{
            "job_clusters": [
                {"job_cluster_key": "some", "new_cluster": {}},
            ]
        }
    )
    with pytest.raises(ValueError):
        jc.get_cluster_definition("non-existent")


def test_positive():
    nc_content = {"node_type_id": "some-node-type-id"}
    jc = JobClusters(
        **{
            "job_clusters": [
                {"job_cluster_key": "some", "new_cluster": nc_content},
            ]
        }
    )
    assert jc.get_cluster_definition("some") is not None
    assert jc.get_cluster_definition("some").job_cluster_key == "some"
    assert jc.get_cluster_definition("some").new_cluster == nc_content
