import pytest

from dbx.models.workflow.v2dot0.workflow import Workflow


def test_wf(capsys):
    wf = Workflow(existing_cluster_name="something", pipeline_task={"pipeline_id": "something"}, name="test")
    assert "cluster://" in capsys.readouterr().out
    with pytest.raises(RuntimeError):
        wf.get_task("whatever")


@pytest.mark.parametrize(
    "wf_def",
    [
        {
            "name": "test1",
            "new_cluster": {"spark_version": "lts"},
            "existing_cluster_name": "some-cluster",
            "some_task": "here",
        }
    ],
)
def test_validation(wf_def):
    with pytest.raises(ValueError):
        Workflow(**wf_def)
