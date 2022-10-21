from pathlib import Path

import pytest

from dbx.models.workflow.common.task import SparkPythonTask
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


def test_sql_task_non_unique():
    payload = {"query": {"query_id": "some"}, "dashboard": {"dashboard_id": "some"}, "warehouse_id": "some"}
    with pytest.raises(ValueError):
        SqlTask(**payload)


def test_sql_task_good():
    payload = {"query": {"query_id": "some"}, "warehouse_id": "some"}
    _task = SqlTask(**payload)
    assert _task.query.query_id is not None
