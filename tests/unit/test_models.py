import pytest

from dbx.models.base import FlexibleBaseModel
from dbx.models.deployment import Deployment
from dbx.models.tasks import DependencyDefinition, TaskDefinition


def test_flexible_base_model():
    class TestingModel(FlexibleBaseModel):
        id: str

    extras = {"extra_field": 10, "nested_extra_field": [1, 2, 3]}
    td = TestingModel(id="1", **extras)
    assert td.extra == extras


def test_positive_model():
    sample_deployment = Deployment(
        **{
            "environments": {
                "default": {
                    "workloads": [
                        {
                            "name": "main",
                            "tasks": [
                                {"task_key": "first", "spark_python_task": {"python_file": "file://some-file.py"}},
                                {
                                    "task_key": "second",
                                    "notebook_task": {"notebook_path": "/Repos/some-project/notebook"},
                                    "max_retries": "10",
                                    "depends_on": [{"task_key": "first"}],
                                },
                                {
                                    "task_key": "third",
                                    "spark_submit_task": {"parameters": ["some", "parameter"]},
                                    "depends_on": [{"task_key": "first"}, {"task_key": "second"}],
                                },
                            ],
                        }
                    ]
                }
            }
        }
    )
    assert len(sample_deployment.environments) == 1
    assert len(sample_deployment.environments["default"].workloads) == 1
    assert sample_deployment.environments["default"].workloads[0].name == "main"
    assert len(sample_deployment.environments["default"].workloads[0].tasks) == 3
    assert sample_deployment.environments["default"].workloads[0].tasks[0].task_key == "first"
    assert sample_deployment.environments["default"].workloads[0].tasks[1].extra == {"max_retries": "10"}
    assert sample_deployment.environments["default"].workloads[0].tasks[2].depends_on == [
        DependencyDefinition(task_key="first"),
        DependencyDefinition(task_key="second"),
    ]


def test_negative_task_model():
    with pytest.raises(Exception):
        badly_configured_task = TaskDefinition(**{"task_key": "something"})
