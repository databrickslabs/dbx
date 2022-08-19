import pytest

from dbx.models.parameters.execute import ExecuteWorkloadParamInfo
from dbx.models.parameters.run_now import RunNowV2d0ParamInfo, RunNowV2d1ParamInfo
from dbx.models.parameters.run_submit import RunSubmitV2d0ParamInfo, RunSubmitV2d1ParamInfo


def test_empty_execute():
    with pytest.raises(ValueError):
        ExecuteWorkloadParamInfo(**{})


def test_multiple_execute():
    with pytest.raises(ValueError):
        ExecuteWorkloadParamInfo(**{"parameters": ["a", "b"], "named_parameters": ["--c=1"]})


def test_params_execute():
    _p = ExecuteWorkloadParamInfo(**{"parameters": ["a", "b"]})
    assert _p.parameters is not None


def test_named_params_execute():
    _p = ExecuteWorkloadParamInfo(**{"named_parameters": ["--a=1", "--b=2"]})
    assert _p.named_parameters is not None


def test_runnow_v20():
    _rn = RunNowV2d0ParamInfo(**{"jar_params": ["a"]})
    assert _rn.jar_params is not None


def test_runnow_v20_two():
    _rn = RunNowV2d0ParamInfo(**{"jar_params": ["a"], "notebook_params": {"a": 1}})
    assert _rn.jar_params is not None
    assert _rn.notebook_params is not None


def test_runnow_v20_negative():
    with pytest.raises(ValueError):
        RunNowV2d0ParamInfo(**{})


def test_runnow_v21():
    _rn = RunNowV2d1ParamInfo(**{"python_named_params": {"a": 1}})
    assert _rn.python_named_params is not None


def test_runsubmit_v20():
    _rs = RunSubmitV2d0ParamInfo(**{"spark_python_task": {"parameters": ["a"]}})
    assert _rs.spark_python_task.parameters is not None


def test_runsubmit_v20_non_unique():
    with pytest.raises(ValueError):
        RunSubmitV2d0ParamInfo(**{"spark_python_task": {"parameters": ["a"]}, "spark_jar_task": {"parameters": ["a"]}})


def test_runsubmit_v20_empty():
    with pytest.raises(ValueError):
        RunSubmitV2d0ParamInfo(**{})


def test_runsubmit_v21_empty_tasks():
    with pytest.raises(ValueError):
        RunSubmitV2d0ParamInfo(**{"tasks": []})


def test_runsubmit_v21_empty():
    with pytest.raises(ValueError):
        RunSubmitV2d1ParamInfo(**{})


def test_runsubmit_v21_good():
    _sp_task = {"task_key": "first", "spark_python_task": {"parameters": ["a"]}}
    _sj_task = {"task_key": "second", "spark_jar_task": {"parameters": ["a"]}}
    _rs = RunSubmitV2d1ParamInfo(**{"tasks": [_sp_task, _sj_task]})

    assert _rs.tasks[0].task_key == "first"
    assert _rs.tasks[0].spark_python_task.parameters is not None

    assert _rs.tasks[1].task_key == "second"
    assert _rs.tasks[1].spark_jar_task.parameters is not None
