import pytest
from pydantic import ValidationError

from dbx.models.workflow.v2dot1.parameters import AssetBasedRunPayload
from dbx.models.workflow.v2dot1.workflow import Workflow


def test_empty_tasks(capsys):
    Workflow(tasks=[], name="empty")
    assert "might cause errors" in capsys.readouterr().out


def test_task_not_provided():
    with pytest.raises(ValidationError):
        Workflow(name="test", tasks=[{"some": "a"}, {"other": "b"}])


def test_duplicated_tasks(capsys):
    with pytest.raises(ValidationError):
        Workflow(tasks=[{"task_key": "d", "some_task": "prop"}, {"task_key": "d", "some_task": "prop"}], name="empty")


def test_override_positive():
    wf = Workflow(
        name="some",
        tasks=[{"task_key": "one", "spark_python_task": {"python_file": "/some/file.py", "parameters": ["a"]}}],
    )
    override_payload = AssetBasedRunPayload.from_string(
        """[
    {"task_key": "one", "parameters": ["a", "b"]}
    ]"""
    )
    wf.override_asset_based_launch_parameters(override_payload)
    assert wf.get_task("one").spark_python_task.parameters == ["a", "b"]
    assert wf.task_names == ["one"]
