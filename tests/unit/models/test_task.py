from copy import deepcopy
from pathlib import Path

import pytest
from pydantic import ValidationError

from dbx.models.task import Task, TaskType, SparkPythonTask


def get_spark_python_task_payload(py_file: str):
    spark_python_task_payload = {
        "spark_python_task": {
            "python_file": py_file,
            "parameters": ["--conf-file", "file:fuse://conf/test/sample_ml_config.yml"],
        }
    }
    return spark_python_task_payload


python_wheel_task_payload = {
    "python_wheel_task": {
        "package_name": "some-pkg",
        "entry_point": "etl",
        "parameters": ["--conf-file", "file:fuse://conf/test/sample_etl_config.yml"],
    }
}


def test_spark_python_task_positive(temp_project: Path):
    py_file = f"file://{temp_project.name}/tasks/sample_etl_task.py"
    _payload = get_spark_python_task_payload(py_file).get("spark_python_task")
    _t = SparkPythonTask(**_payload)
    assert isinstance(_t.python_file, Path)


def test_task_recognition(temp_project: Path):
    py_file = f"file://{temp_project.name}/tasks/sample_etl_task.py"
    _payload = get_spark_python_task_payload(py_file)
    _result = Task(**_payload)
    assert _result.spark_python_task is not None
    assert _result.python_wheel_task is None
    assert _result.task_type == TaskType.spark_python_task


def test_python_wheel_task():
    _result = Task(**python_wheel_task_payload)
    assert _result.spark_python_task is None
    assert _result.python_wheel_task is not None
    assert _result.task_type == TaskType.python_wheel_task


def test_python_wheel_task_named():
    _c = deepcopy(python_wheel_task_payload)
    _c["python_wheel_task"].pop("parameters")
    _c["python_wheel_task"]["named_parameters"] = ["--a=1", "--b=2"]
    _result = Task(**_c)
    assert _result.task_type == TaskType.python_wheel_task
    assert _result.python_wheel_task.named_parameters is not None


def test_python_wheel_task_named_invalid_prefix():
    _c = deepcopy(python_wheel_task_payload)
    _c["python_wheel_task"].pop("parameters")
    _c["python_wheel_task"]["named_parameters"] = ["a", "--b=2"]
    with pytest.raises(ValidationError):
        Task(**_c)


def test_python_wheel_task_named_invalid_equal():
    _c = deepcopy(python_wheel_task_payload)
    _c["python_wheel_task"].pop("parameters")
    _c["python_wheel_task"]["named_parameters"] = ["--a"]
    with pytest.raises(ValidationError):
        Task(**_c)


def test_negative():
    _payload = {"spark_jar_task": {"main_class_name": "org.some.Class"}}

    with pytest.raises(ValueError):
        Task(**_payload)


def test_multiple(temp_project):
    py_file = f"file://{temp_project.name}/tasks/sample_etl_task.py"
    _sp_payload = get_spark_python_task_payload(py_file)
    _payload = {**_sp_payload, **python_wheel_task_payload}
    with pytest.raises(ValueError):
        Task(**_payload)
