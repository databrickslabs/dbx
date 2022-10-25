from typing import Optional

from dbx.models.workflow.common.task import (
    BaseNotebookTask,
    BasePipelineTask,
    BaseTaskMixin,
    SparkJarTask,
    SparkPythonTask,
    SparkSubmitTask,
)


class NotebookTask(BaseNotebookTask):
    revision_timestamp: Optional[int]


class PipelineTask(BasePipelineTask):
    """Simple reference to the base"""


class TaskMixin(BaseTaskMixin):
    notebook_task: Optional[NotebookTask]
    spark_jar_task: Optional[SparkJarTask]
    spark_python_task: Optional[SparkPythonTask]
    spark_submit_task: Optional[SparkSubmitTask]
    pipeline_task: Optional[PipelineTask]
