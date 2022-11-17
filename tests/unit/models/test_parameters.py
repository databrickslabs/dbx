import pytest

from dbx.models.workflow.v2dot0.parameters import StandardRunPayload as V2dot0StandardRunPayload
from dbx.models.workflow.v2dot0.parameters import AssetBasedRunPayload as V2dot0AssetBasedRunPayload
from dbx.models.workflow.v2dot1.parameters import StandardRunPayload as V2dot1StandardRunPayload
from dbx.models.workflow.v2dot1.parameters import AssetBasedRunPayload as V2dot1AssetBasedRunPayload
from dbx.models.cli.execute import ExecuteParametersPayload


def test_empty_execute():
    with pytest.raises(ValueError):
        ExecuteParametersPayload(**{})


def test_multiple_execute():
    with pytest.raises(ValueError):
        ExecuteParametersPayload(**{"parameters": ["a", "b"], "named_parameters": ["--c=1"]})


def test_params_execute():
    _p = ExecuteParametersPayload(**{"parameters": ["a", "b"]})
    assert _p.parameters is not None


def test_named_params_execute():
    _p = ExecuteParametersPayload(**{"named_parameters": {"a": 1}})
    assert _p.named_parameters is not None


def test_standard_v2dot0():
    _rn = V2dot0StandardRunPayload(**{"jar_params": ["a"]})
    assert _rn.jar_params is not None


def test_standard_v2dot0_multiple():
    _rn = V2dot0StandardRunPayload(**{"jar_params": ["a"], "notebook_params": {"a": 1}})
    assert _rn.jar_params is not None
    assert _rn.notebook_params is not None


def test_standard_v2dot1():
    _rn = V2dot1StandardRunPayload(**{"python_named_params": {"a": 1}})
    assert _rn.python_named_params is not None


def test_assert_based_v2dot0():
    _rs = V2dot0AssetBasedRunPayload(**{"parameters": ["a"]})
    assert _rs.parameters is not None


def test_assert_based_v2dot0_not_unique():
    with pytest.raises(ValueError):
        V2dot0AssetBasedRunPayload(**{"parameters": ["a"], "base_parameters": {"a": "b"}})


@pytest.mark.parametrize(
    "param_raw_payload",
    [
        '[{"task_key": "some", "base_parameters": {"a": 1, "b": 2}}]',
        '[{"task_key": "some", "parameters": ["a", "b"]}]',
        '[{"task_key": "some", "named_parameters": {"a": 1}}]',
        '[{"task_key": "some", "full_refresh": true}]',
        '[{"task_key": "some", "parameters": {"key1": "value2"}}]',
    ],
)
def test_assert_based_v2dot1_good(param_raw_payload):
    parsed = V2dot1AssetBasedRunPayload.from_string(param_raw_payload)
    assert parsed.elements is not None
