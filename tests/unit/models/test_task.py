from pathlib import Path

from dbx.models.workflow.common.task import SparkPythonTask


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
