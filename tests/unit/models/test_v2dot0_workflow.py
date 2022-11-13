import pytest

from dbx.models.workflow.v2dot0.parameters import AssetBasedRunPayload
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


def test_v2dot0_overrides_parameters():
    wf = Workflow(**{"name": "test", "spark_python_task": {"python_file": "some/file.py", "parameters": ["a"]}})
    wf.override_asset_based_launch_parameters(AssetBasedRunPayload(parameters=["b"]))
    assert wf.spark_python_task.parameters == ["b"]


def test_v2dot0_overrides_notebook():
    wf = Workflow(**{"name": "test", "notebook_task": {"notebook_path": "/some/path", "base_parameters": {"k1": "v1"}}})
    wf.override_asset_based_launch_parameters(AssetBasedRunPayload(base_parameters={"k1": "v2"}))
    assert wf.notebook_task.base_parameters == {"k1": "v2"}
