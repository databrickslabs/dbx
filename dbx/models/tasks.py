from typing import List, Optional, Dict, Any

from pydantic import root_validator

from dbx.models.base import FlexibleBaseModel, CustomProperties
from dbx.models.clusters import NewCluster
from dbx.models.libraries import Library


class DependencyDefinition(FlexibleBaseModel):
    task_key: str


class NotebookTask(FlexibleBaseModel):
    notebook_path: str
    base_parameters: Optional[Dict[str, Any]]


class SparkJarTask(FlexibleBaseModel):
    main_class_name: str
    parameters: Optional[List[str]]


class SparkPythonTask(FlexibleBaseModel):
    python_file: str
    parameters: Optional[List[str]]


class SparkSubmitTask(FlexibleBaseModel):
    parameters: List[str]


class PipelineTask(FlexibleBaseModel):
    pipeline_id: str


class PythonWheelTask(FlexibleBaseModel):
    package_name: str
    entry_point: str
    parameters: Optional[List[str]]
    named_parameters: Optional[Dict[str, Any]]


class TaskDefinition(FlexibleBaseModel):
    task_key: str
    depends_on: Optional[List[DependencyDefinition]]
    custom_properties: Optional[CustomProperties]
    libraries: Optional[List[Library]]

    notebook_task: Optional[NotebookTask]
    spark_jar_task: Optional[SparkJarTask]
    spark_python_task: Optional[SparkPythonTask]
    spark_submit_task: Optional[SparkSubmitTask]
    pipeline_task: Optional[PipelineTask]
    python_wheel_task: Optional[PythonWheelTask]

    # cluster properties might be also defined on the task level
    new_cluster: Optional[NewCluster]

    existing_cluster_id: Optional[str]
    existing_cluster_name: Optional[str]  # this field is used in the named properties' resolution logic

    class Config:
        SUPPORTED_TASK_DEFINITIONS = [
            "notebook_task",
            "spark_jar_task",
            "spark_python_task",
            "spark_submit_task",
            "pipeline_task",
            "python_wheel_task",
        ]

    @root_validator()
    def check_task(cls, values: Dict[str, Any]):  # noqa
        """
        This validator checks if at least one task definitions has been provided
        :param values: all values of the model
        :return: all values of the model
        """
        task_items = [values.get(k) for k in cls.__config__.SUPPORTED_TASK_DEFINITIONS]
        if all(t is None for t in task_items):
            raise Exception(
                f"No task launch definition was provided for task {values['task_key']}. "
                f"Please provide one of {cls.__config__.SUPPORTED_TASK_DEFINITIONS}"
            )
        return values
