import pytest

from dbx.models.parameters import ExecuteWorkloadParamInfo


def test_empty_v20():
    with pytest.raises(ValueError):
        ExecuteWorkloadParamInfo(**{})


def test_multiple():
    with pytest.raises(ValueError):
        ExecuteWorkloadParamInfo(**{"parameters": ["a", "b"], "named_parameters": ["--c=1"]})


def test_unsupported():
    with pytest.raises(ValueError):
        ExecuteWorkloadParamInfo(**{"parameters": ["a", "b"], "named_parameters": ["--c=1"]})


def test_params():
    _p = ExecuteWorkloadParamInfo(**{"parameters": ["a", "b"]})
    assert _p.parameters is not None


def test_named_params():
    _p = ExecuteWorkloadParamInfo(**{"named_parameters": ["--a=1", "--b=2"]})
    assert _p.named_parameters is not None
