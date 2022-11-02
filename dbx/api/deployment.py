from databricks_cli.sdk import ApiClient

from dbx.api.adjuster.mixins.base import ApiClientMixin
from dbx.api.services.jobs import NamedJobsService
from dbx.api.services.permissions import PermissionsService
from dbx.api.services.pipelines import NamedPipelinesService
from dbx.models.deployment import WorkflowList, AnyWorkflow
from dbx.models.workflow.common.pipeline import Pipeline
from dbx.utils import dbx_echo


class WorkflowDeploymentManager(ApiClientMixin):
    def __init__(self, api_client: ApiClient, workflows: WorkflowList):
        super().__init__(api_client)
        self._wfs = workflows
        self._deployment_data = {}

    def _apply_permissions(self, wf: AnyWorkflow):
        PermissionsService(self.api_client).apply(wf)

    def _deploy(self, wf: AnyWorkflow):
        operator_service = NamedPipelinesService if isinstance(wf, Pipeline) else NamedJobsService
        service_instance = operator_service(self.api_client)
        obj_id = service_instance.find_by_name(wf.name)

        if not obj_id:
            service_instance.create(wf)
        else:
            service_instance.update(obj_id, wf)

    def apply(self):
        dbx_echo("🤖 Applying workflow definitions via API")

        for wf in self._wfs:
            self._deploy(wf)
            self._apply_permissions(wf)

        dbx_echo("✅ Applying workflow definitions - done")
