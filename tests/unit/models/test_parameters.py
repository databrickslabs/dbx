import pytest

from dbx.models.parameters import ExecuteWorkloadParamInfo, LaunchWorkloadParamInfo


def test_empty_execute():
    with pytest.raises(ValueError):
        ExecuteWorkloadParamInfo(**{})


def test_multiple_execute():
    with pytest.raises(ValueError):
        ExecuteWorkloadParamInfo(**{"parameters": ["a", "b"], "named_parameters": ["--c=1"]})


def test_unsupported_execute():
    with pytest.raises(ValueError):
        ExecuteWorkloadParamInfo(**{"parameters": ["a", "b"], "named_parameters": ["--c=1"]})


def test_params_execute():
    _p = ExecuteWorkloadParamInfo(**{"parameters": ["a", "b"]})
    assert _p.parameters is not None


def test_named_params_execute():
    _p = ExecuteWorkloadParamInfo(**{"named_parameters": ["--a=1", "--b=2"]})
    assert _p.named_parameters is not None


def test_empty_launch():
    with pytest.raises(ValueError):
        LaunchWorkloadParamInfo.from_string("{}")


def test_launch_job_top_level():
    _r = LaunchWorkloadParamInfo.from_string('{"parameters": ["argument1", "argument2"]}')
    assert _r.content.parameters is not None


def test_launch_job_task_level():
    _r = LaunchWorkloadParamInfo.from_string('[{"task_key": "some", "named_parameters": ["--a=1", "--b=2"]}]')
    assert _r.content[0].task_key is not None


def test_launch_multiple_tasks():
    _r = LaunchWorkloadParamInfo.from_string(
        """
            [
                {"task_key": "first", "base_parameters": {"a": 1, "b": 2}},
                {"task_key": "second", "parameters": ["a", "b"]}
            ]
        """
    )
    assert _r.content[0].task_key == "first"
    assert _r.content[0].base_parameters is not None
    assert _r.content[1].task_key == "second"
    assert _r.content[1].parameters is not None
