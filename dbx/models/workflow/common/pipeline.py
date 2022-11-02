from typing import Optional, Dict, List, Any, Literal

from pydantic import validator

from dbx.models.workflow.common.access_control import AccessControlMixin
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.new_cluster import NewCluster
from dbx.utils import dbx_echo


class PipelinesNewCluster(NewCluster):
    label: Optional[str]
    spark_version: Optional[str] = None
    init_scripts: List[Any] = []

    @staticmethod
    def _omit_msg(property_name: str):
        dbx_echo(
            f"[yellow bold]The `{property_name}` property cannot be applied for DLT pipelines. "
            "Provided value will be omitted.[/yellow bold]"
        )

    @validator("init_scripts", pre=True)
    def _validate_init_scripts(cls, value):  # noqa
        if value:
            cls._omit_msg("init_scripts")
        return []

    @validator("spark_version", pre=True)
    def _validate_spark_version(cls, value):  # noqa
        if value:
            cls._omit_msg("spark_version")


class NotebookLibrary(FlexibleModel):
    path: str


class PipelineLibrary(FlexibleModel):
    notebook: NotebookLibrary


class Pipeline(AccessControlMixin):
    name: str
    pipeline_id: Optional[str]
    workflow_type: Literal["pipeline"]
    storage: Optional[str]
    target: Optional[str]
    configuration: Optional[Dict[str, str]]
    clusters: Optional[List[PipelinesNewCluster]] = []
    libraries: Optional[List[PipelineLibrary]] = []
