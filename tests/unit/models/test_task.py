from pathlib import Path

import pytest
from pydantic import ValidationError

from dbx.models.cli.execute import ExecuteParametersPayload
from dbx.models.workflow.common.task import SparkPythonTask, SparkJarTask, SparkSubmitTask, BaseTaskMixin
from dbx.models.workflow.common.task_type import TaskType
from dbx.models.workflow.v2dot1.task import SqlTask


def get_spark_python_task_payload(py_file: str):
    spark_python_task_payload = {
        "spark_python_task": {
            "python_file": py_file,
            "parameters": ["--conf-file", "file:fuse://conf/tasks/sample_ml_config.yml"],
        }
    }
    return spark_python_task_payload


python_wheel_task_payload = {
    "python_wheel_task": {
        "package_name": "some-pkg",
        "entry_point": "etl",
        "parameters": ["--conf-file", "file:fuse://conf/tasks/sample_etl_config.yml"],
    }
}


def test_spark_python_task_positive(temp_project: Path):
    py_file = f"file://{temp_project.name}/tasks/sample_etl_task.py"
    _payload = get_spark_python_task_payload(py_file).get("spark_python_task")
    _t = SparkPythonTask(**_payload)
    assert isinstance(_t.execute_file, Path)


def test_spark_python_task_non_py_file(temp_project: Path):
    py_file = f"file://{temp_project.name}/tasks/sample_etl_task.ipynb"
    _payload = get_spark_python_task_payload(py_file).get("spark_python_task")
    with pytest.raises(ValidationError):
        SparkPythonTask(**_payload)


def test_sql_task_non_unique():
    payload = {"query": {"query_id": "some"}, "dashboard": {"dashboard_id": "some"}, "warehouse_id": "some"}
    with pytest.raises(ValueError):
        SqlTask(**payload)


def test_sql_task_good():
    payload = {"query": {"query_id": "some"}, "warehouse_id": "some"}
    _task = SqlTask(**payload)
    assert _task.query.query_id is not None


def test_spark_jar_deprecated(capsys):
    _jt = SparkJarTask(main_class_name="some.Class", jar_uri="file://some/uri")
    assert _jt.jar_uri is not None
    assert "Field jar_uri is DEPRECATED since" in capsys.readouterr().out


def test_spark_python_task_not_fuse(temp_project: Path):
    py_file = f"file://{temp_project.name}/tasks/sample_etl_task.py"
    _payload = get_spark_python_task_payload(py_file).get("spark_python_task")
    _payload["python_file"] = "file:fuse://some/file"
    with pytest.raises(ValueError):
        SparkPythonTask(**_payload)


def test_spark_python_task_execute_incorrect(temp_project: Path):
    py_file = f"file://{temp_project.name}/tasks/sample_etl_task.py"
    _payload = get_spark_python_task_payload(py_file).get("spark_python_task")
    _payload["python_file"] = "dbfs:/some/path.py"
    with pytest.raises(ValueError):
        _st = SparkPythonTask(**_payload)
        _st.execute_file  # noqa


def test_spark_python_task_execute_non_existent(temp_project: Path):
    py_file = f"file://{temp_project.name}/tasks/sample_etl_task.py"
    _payload = get_spark_python_task_payload(py_file).get("spark_python_task")
    _t = SparkPythonTask(**_payload)
    Path(py_file.replace("file://", "")).unlink()
    with pytest.raises(ValueError):
        _st = SparkPythonTask(**_payload)
        _st.execute_file  # noqa


def test_spark_submit_task():
    st = SparkSubmitTask(**{"parameters": ["some", "other"]})
    assert st.parameters is not None


def test_mixin_undefined_type():
    bt = BaseTaskMixin(**{"unknown_task": {"prop1": "arg1"}})
    assert bt.task_type == TaskType.undefined_task


def test_mixin_execute_unsupported():
    bt = BaseTaskMixin(**{"unknown_task": {"prop1": "arg1"}})
    with pytest.raises(RuntimeError):
        bt.check_if_supported_in_execute()


def test_mixin_incorrect_override():
    bt = BaseTaskMixin(**{"spark_python_task": {"python_file": "/some/file"}})
    with pytest.raises(ValueError):
        bt.override_execute_parameters(ExecuteParametersPayload(named_parameters={"p1": 1}))


def test_mixin_multiple_provided():
    with pytest.raises(ValueError):
        BaseTaskMixin(**{"spark_python_task": {"python_file": "/some/file"}, "unknown_task": {"some": "props"}})
