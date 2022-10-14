from unittest.mock import MagicMock

import pytest
from databricks_cli.sdk import JobsService
from pytest_mock import MockerFixture

from dbx.api.configure import ProjectConfigurationManager
from dbx.api.launch.runners import RunSubmitLauncher
from dbx.models.parameters.run_submit import RunSubmitV2d1ParamInfo
from dbx.models.workflow.v2d0.parameters import RunSubmitV2d0ParamInfo


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


def test_run_submit_reuse(temp_project, mocker: MockerFixture):
    ProjectConfigurationManager().enable_failsafe_cluster_reuse()
    service_mock = mocker.patch.object(JobsService, "submit_run", MagicMock())
    cluster_def = {"some_key": "some_value"}
    mocker.patch(
        "dbx.api.launch.runners.load_dbx_file",
        MagicMock(
            return_value={
                "default": {
                    "jobs": [
                        {
                            "name": "test",
                            "job_clusters": [{"job_cluster_key": "some", "new_cluster": cluster_def}],
                            "tasks": [{"task_key": "one", "job_cluster_key": "some"}],
                        }
                    ]
                }
            }
        ),
    )
    # code below patches the _cleanup_unsupported_properties method and simply returns the same input
    # _cleanup_unsupported_properties is tested below, here we omit it due to the mocking logic
    mocker.patch.object(RunSubmitLauncher, "_cleanup_unsupported_properties", MagicMock(side_effect=lambda a: a))
    launcher = RunSubmitLauncher(job="test", api_client=MagicMock(), deployment_run_id="aaa-bbb", environment="default")
    launcher.launch()
    service_mock.assert_called_once_with(tasks=[{"task_key": "one", "new_cluster": cluster_def}])


def test_unsupported_properties(capsys):
    spec = {"max_retries": 2, "tasks": [{"spark_python_task": {"python_file": "some/file"}}]}
    RunSubmitLauncher._cleanup_unsupported_properties(spec)
    res = capsys.readouterr()
    assert "Property max_retries is not supported in the assets-only launch mode" in res.out
