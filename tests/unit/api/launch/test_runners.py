import pytest

from dbx.api.launch.runners import RunSubmitLauncher
from dbx.models.parameters.run_submit import RunSubmitV2d0ParamInfo, RunSubmitV2d1ParamInfo


def test_v2d0_parameter_override_negative():
    spec = {"spark_python_task": {"parameters": ["a"]}}
    parameters = RunSubmitV2d0ParamInfo(notebook_task={"base_parameters": {"a": 1}})
    with pytest.raises(ValueError):
        RunSubmitLauncher.override_v2d0_parameters(spec, parameters)


def test_v2d0_parameter_override_positive():
    spec = {"spark_python_task": {"parameters": ["a"]}}
    parameters = RunSubmitV2d0ParamInfo(spark_python_task={"parameters": ["b"]})
    RunSubmitLauncher.override_v2d0_parameters(spec, parameters)
    assert spec["spark_python_task"]["parameters"] == ["b"]


def test_vd21_parameter_override_no_tasks():
    spec = {"a": "b"}
    parameters = RunSubmitV2d1ParamInfo(tasks=[{"task_key": "first", "spark_python_task": {"parameters": ["a"]}}])
    with pytest.raises(ValueError):
        RunSubmitLauncher.override_v2d1_parameters(spec, parameters)


def test_vd21_parameter_override_no_task_key():
    spec = {"tasks": [{"task_key": "this", "spark_python_task": {"parameters": ["a"]}}]}
    parameters = RunSubmitV2d1ParamInfo(tasks=[{"task_key": "that", "spark_python_task": {"parameters": ["a"]}}])
    with pytest.raises(ValueError):
        RunSubmitLauncher.override_v2d1_parameters(spec, parameters)


def test_vd21_parameter_override_incorrect_type():
    spec = {"tasks": [{"task_key": "this", "python_wheel_task": {"parameters": ["a"]}}]}
    parameters = RunSubmitV2d1ParamInfo(tasks=[{"task_key": "this", "spark_python_task": {"parameters": ["a"]}}])
    with pytest.raises(ValueError):
        RunSubmitLauncher.override_v2d1_parameters(spec, parameters)


def test_vd21_parameter_override_positive():
    spec = {"tasks": [{"task_key": "this", "python_wheel_task": {"parameters": ["a"]}}]}
    parameters = RunSubmitV2d1ParamInfo(tasks=[{"task_key": "this", "python_wheel_task": {"parameters": ["b"]}}])
    RunSubmitLauncher.override_v2d1_parameters(spec, parameters)
    assert spec["tasks"][0]["python_wheel_task"]["parameters"] == ["b"]
