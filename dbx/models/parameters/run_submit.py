from __future__ import annotations

from abc import abstractmethod
from typing import List, Optional, Union

from pydantic import BaseModel, validator, root_validator

from dbx.models.parameters.common import ParamPair, StringArray, validate_contains, validate_unique
from dbx.models.task import validate_named_parameters


class BaseTaskModel(BaseModel):
    @abstractmethod
    def get_parameters_key(self) -> str:
        """"""

    @abstractmethod
    def get_parameters(self) -> Union[ParamPair, StringArray]:
        """"""


class NotebookTask(BaseTaskModel):
    base_parameters: ParamPair

    def get_parameters_key(self) -> str:
        return "base_parameters"

    def get_parameters(self) -> Union[ParamPair, StringArray]:
        return self.base_parameters


class SparkJarTask(BaseTaskModel):
    parameters: StringArray

    def get_parameters_key(self) -> str:
        return "parameters"

    def get_parameters(self) -> Union[ParamPair, StringArray]:
        return self.parameters


class SparkPythonTask(BaseModel):
    parameters: StringArray

    def get_parameters_key(self) -> str:  # noqa
        return "parameters"

    def get_parameters(self) -> Union[ParamPair, StringArray]:
        return self.parameters


class SparkSubmitTask(BaseModel):
    parameters: StringArray

    def get_parameters_key(self) -> str:  # noqa
        return "parameters"

    def get_parameters(self) -> Union[ParamPair, StringArray]:
        return self.parameters


class PythonWheelTask(BaseTaskModel):
    parameters: StringArray
    named_parameters: ParamPair

    @root_validator(pre=True)
    def initialize(cls, values):  # noqa
        validate_contains(cls.__fields__, values)
        validate_unique(cls.__fields__, values)
        return values

    @validator("named_parameters", pre=True)
    def _validate_named_parameters(cls, values: List[str]):  # noqa
        validate_named_parameters(values)
        return values

    def get_parameters_key(self) -> str:
        _key = "parameters" if self.parameters else "named_parameters"
        return _key

    def get_parameters(self) -> Union[ParamPair, StringArray]:
        _params = self.parameters if self.parameters else self.named_parameters
        return _params


class TaskContainerModel(BaseModel):
    def get_task_key(self) -> str:
        """
        Returns the name of the non-empty task section
        """
        _task_key = [
            k
            for k in self.dict(exclude_none=True, exclude_unset=True, exclude_defaults=True).keys()
            if k.endswith("_task")
        ]
        return _task_key[0]

    def get_defined_task(self) -> Union[NotebookTask, SparkJarTask, SparkPythonTask, SparkSubmitTask]:
        return getattr(self, self.get_task_key())


class RunSubmitV2d0ParamInfo(TaskContainerModel):
    notebook_task: Optional[NotebookTask]
    spark_jar_task: Optional[SparkJarTask]
    spark_python_task: Optional[SparkPythonTask]
    spark_submit_task: Optional[SparkSubmitTask]

    @root_validator(pre=True)
    def initialize(cls, values):  # noqa
        validate_contains(cls.__fields__, values)
        validate_unique(cls.__fields__, values)
        return values


class NamedV2d1Task(TaskContainerModel):
    task_key: str
    notebook_task: Optional[NotebookTask]
    spark_jar_task: Optional[SparkJarTask]
    spark_python_task: Optional[SparkPythonTask]
    spark_submit_task: Optional[SparkSubmitTask]
    python_wheel_task: Optional[PythonWheelTask]

    @root_validator(pre=True)
    def initialize(cls, values):  # noqa
        task_fields = {k: v for k, v in cls.__fields__.items() if k != "task_key"}
        validate_contains(task_fields, values)
        validate_unique(task_fields, values)
        return values


class RunSubmitV2d1ParamInfo(BaseModel):
    tasks: List[NamedV2d1Task]
