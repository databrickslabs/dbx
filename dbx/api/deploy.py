import enum
import pathlib
import shutil
import tempfile
from typing import Optional, Dict, List, Any

import mlflow

from dbx.api.configure import ProjectConfigurationManager
from dbx.api.jobs import AdvancedJobsService
from dbx.api.mlflow import MlflowStorageConfigurationManager
from dbx.models.deployment import WorkloadDefinition
from dbx.utils import dbx_echo
from dbx.utils.adjuster import AdjustmentManager
from dbx.utils.file_uploader import MlflowFileUploader
from dbx.utils.json import JsonUtils


class DeploymentKind(enum.Enum):
    JOB = "JOB"
    SNAPSHOT = "SNAPSHOT"


class DeploymentManager:
    def __init__(
        self,
        workloads: List[WorkloadDefinition],
        env_name: str,
        additional_tags: Optional[Dict[str, str]],
        save_final_definitions: Optional[pathlib.Path],
        kind: DeploymentKind = DeploymentKind.JOB,
    ):
        self._workloads = workloads
        self._additional_tags = additional_tags
        self._kind = kind
        self._env_name = env_name
        self.storage_info = ProjectConfigurationManager().get(env_name)
        self._local_final_definitions_path = save_final_definitions
        self.prepare_storage()

    def prepare_storage(self):
        if self.storage_info.storage_type == "mlflow":
            MlflowStorageConfigurationManager(self.storage_info.properties).prepare()

    def _adjust_and_upload(self, base_uri: str) -> List[WorkloadDefinition]:
        uploader = MlflowFileUploader(base_uri)
        adjuster = AdjustmentManager(self._workloads, uploader)
        return adjuster.adjust_workloads()

    @staticmethod
    def log_content_to_artifact_storage(content: Dict[Any, Any], name: str):
        temp_dir = tempfile.mkdtemp()
        temp_file = pathlib.Path(temp_dir, name)
        JsonUtils.write(temp_file, content)
        mlflow.log_artifact(str(temp_file), ".dbx")
        shutil.rmtree(temp_dir)

    def _create_job_definitions(self, workloads: List[WorkloadDefinition]):
        dbx_echo("Updating job definitions")
        service = AdvancedJobsService()
        name_to_id_mapping = service.create_jobs_and_get_id_mapping(workloads)
        self.log_content_to_artifact_storage(name_to_id_mapping, "deployments.json")
        dbx_echo("Updating job definitions - done")

    def _setup_deployment_tags(self):
        deployment_tags = {
            "dbx_action_type": "deploy",
            "dbx_environment": self._env_name,
            "dbx_status": "SUCCESS",
            "dbx_kind": self._kind,
        }
        deployment_tags.update(self._additional_tags)
        mlflow.set_tags(deployment_tags)

    def _get_final_deployment_definition(self, workloads: List[WorkloadDefinition]):
        deployment_definition = {self._env_name: [w.dict() for w in workloads]}
        return deployment_definition

    def deploy(self):
        _run = mlflow.start_run()
        workloads = self._adjust_and_upload(_run.info.artifact_uri)

        if self._kind == DeploymentKind.JOB:
            self._create_job_definitions(workloads)
        else:
            dbx_echo("Since a snapshot deployment is performed, no job definitions will be changed.")

        self._setup_deployment_tags()

        final_definition = self._get_final_deployment_definition(workloads)
        self.log_content_to_artifact_storage(final_definition, "deployment-result.json")

        if self._local_final_definitions_path:
            JsonUtils.write(self._local_final_definitions_path, final_definition)
