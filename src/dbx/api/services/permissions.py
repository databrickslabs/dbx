from rich.markup import escape

from dbx.api.adjuster.mixins.base import ApiClientMixin
from dbx.models.deployment import AnyWorkflow
from dbx.models.workflow.common.workflow_types import WorkflowType
from dbx.utils import dbx_echo


class PermissionsService(ApiClientMixin):
    def apply(self, wf: AnyWorkflow):
        path = (
            f"/permissions/pipelines/{wf.pipeline_id}"
            if wf.workflow_type == WorkflowType.pipeline
            else f"/permissions/jobs/{wf.job_id}"
        )
        if wf.access_control_list:
            dbx_echo(f"🛂 Applying permission settings for workflow {escape(wf.name)}")
            self.api_client.perform_query(
                "PUT",
                path,
                data=wf.get_acl_payload(),
            )
            dbx_echo(f"✅ Permission settings were successfully set for workflow {escape(wf.name)}")
