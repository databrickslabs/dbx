from abc import ABC
from pathlib import Path
from typing import Optional

from pydantic import validator, root_validator, BaseModel

from dbx.constants import TASKS_SUPPORTED_IN_EXECUTE
from dbx.models.cli.execute import ExecuteParametersPayload
from dbx.models.validators import at_least_one_of, only_one_provided
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.parameters import ParamPair, StringArray
from dbx.models.workflow.common.task_type import TaskType
from dbx.utils import dbx_echo


class BaseNotebookTask(FlexibleModel, ABC):
    notebook_path: str
    base_parameters: Optional[ParamPair]


class SparkJarTask(FlexibleModel):
    main_class_name: str
    parameters: Optional[StringArray]
    jar_params: Optional[StringArray]
    jar_uri: Optional[str]

    @validator("jar_uri")
    def _deprecated_msg(cls, value):  # noqa
        dbx_echo(
            "[yellow bold] Field jar_uri is DEPRECATED since 04/2016. "
            "Provide a [code]jar[/code] through the [code]libraries[/code] field instead."
        )
        return value


class SparkPythonTask(BaseModel):
    python_file: str
    parameters: Optional[StringArray] = []

    @validator("python_file")
    def _not_fuse(cls, v):  # noqa
        if v.startswith("file:fuse://"):
            raise ValueError("The python_file property cannot be FUSE-based")
        return v

    @property
    def execute_file(self) -> Path:
        if not self.python_file.startswith("file://"):
            raise ValueError("File for execute mode should be located locally and referenced via file:// prefix.")

        _path = Path(self.python_file).relative_to("file://")

        if not _path.exists():
            raise ValueError(f"Provided file doesn't exist {_path}")

        return _path


class SparkSubmitTask(FlexibleModel):
    parameters: Optional[StringArray]
    spark_submit_params: Optional[StringArray]

    _validate_provided = root_validator(allow_reuse=True)(
        lambda _, values: at_least_one_of(["parameters", "spark_submit_params"], values)
    )


class BasePipelineTask(FlexibleModel, ABC):
    pipeline_id: str


class BaseTaskMixin(FlexibleModel):
    _only_one_provided = root_validator(pre=True, allow_reuse=True)(
        lambda _, values: only_one_provided("_task", values)
    )

    @property
    def task_type(self) -> TaskType:
        for _type in TaskType:
            if self.dict().get(_type):
                return TaskType(_type)
        return TaskType.undefined_task

    def check_if_supported_in_execute(self):
        if self.task_type not in TASKS_SUPPORTED_IN_EXECUTE:
            raise RuntimeError(
                f"Provided task type {self.task_type} is not supported in execute mode. "
                f"Supported types are: {TASKS_SUPPORTED_IN_EXECUTE}"
            )

    def override_execute_parameters(self, payload: ExecuteParametersPayload):
        if payload.named_parameters and self.task_type == TaskType.spark_python_task:
            raise ValueError(
                "`named_parameters` are not supported by spark_python_task. Please use `parameters` instead."
            )

        pointer = getattr(self, self.task_type)
        pointer.__dict__.update(payload.dict(exclude_none=True))
