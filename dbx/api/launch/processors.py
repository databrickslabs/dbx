from rich.console import Console

from dbx.models.workflow.v2dot1.workflow import Workflow
from dbx.utils import dbx_echo


class ClusterReusePreprocessor:
    @classmethod
    def process(cls, workflow: Workflow):
        with Console().status("🔍 Iterating over task definitions to find shared job cluster usages", spinner="dots"):
            for task in workflow.tasks:
                if task.job_cluster_key is not None:
                    _definition = workflow.get_job_cluster_definition(task.job_cluster_key)
                    task.job_cluster_key = None
                    task.new_cluster = _definition.new_cluster
        workflow.job_clusters = None
        dbx_echo("✅ All shared job cluster usages were replaced with their relevant cluster definitions")
