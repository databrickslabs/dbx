from enum import Enum
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, root_validator, validator


def validate_named_parameters(values: List[str]):
    for v in values:
        if not v.startswith("--"):
            raise ValueError(f"Named parameter shall start with --, provided value: {v}")
        if "=" not in v:
            raise ValueError(f"Named parameter shall contain equal sign, provided value: {v}")


class TaskType(Enum):
    spark_python_task = "spark_python_task"
    python_wheel_task = "python_wheel_task"


class PythonWheelTask(BaseModel):
    package_name: str
    entry_point: str
    parameters: Optional[List[str]] = []
    named_parameters: Optional[List[str]] = []

    @root_validator(pre=True)
    def validate_parameters(cls, values):  # noqa
        if all(param in values for param in ["parameters", "named_parameters"]):
            raise ValueError("Both named_parameters and parameters cannot be provided at the same time")
        return values

    @validator("named_parameters", pre=True)
    def _validate_named_parameters(cls, values: List[str]):  # noqa
        validate_named_parameters(values)
        return values


class SparkPythonTask(BaseModel):
    python_file: Path
    parameters: Optional[List[str]] = []

    @validator("python_file", always=True)
    def python_file_validator(cls, v: Path, values) -> Path:  # noqa
        stripped = v.relative_to("file://")  # we need to strip out the file:// prefix
        if not stripped.exists():
            raise FileNotFoundError(f"File {stripped} is mentioned in the task or job definition, but is non-existent")
        return stripped


class Task(BaseModel):
    spark_python_task: Optional[SparkPythonTask]
    python_wheel_task: Optional[PythonWheelTask]
    task_type: Optional[TaskType]

    @root_validator
    def validate_all(cls, values):  # noqa
        if all(values.get(_type.name) is None for _type in TaskType):
            raise ValueError(
                f"Provided task or job definition doesn't contain one of the supported types: \n"
                f"{[t.value for t in TaskType]}"
            )
        if sum(1 if values.get(_type.name) else 0 for _type in TaskType) > 1:
            raise ValueError("More then one definition has been provided, please review the job or task definition")
        return values

    @validator("task_type", always=True)
    def task_type_validator(cls, v, values) -> TaskType:  # noqa
        for _type in TaskType:
            if values.get(_type.name):
                return TaskType(_type)
