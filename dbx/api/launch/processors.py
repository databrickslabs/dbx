from copy import deepcopy
from typing import Dict, Any

from rich.console import Console

from dbx.models.job_clusters import JobClusters
from dbx.utils import dbx_echo


class ClusterReusePreprocessor:
    def __init__(self, job_spec: Dict[str, Any]):
        self._job_spec = deepcopy(job_spec)
        self._job_clusters = JobClusters(**job_spec)
        # delete the job clusters section from the spec
        self._job_spec.pop("job_clusters")

    def _preprocess_task_definition(self, task: Dict[str, Any]):
        task_cluster_key = task.pop("job_cluster_key")
        definition = self._job_clusters.get_cluster_definition(task_cluster_key).new_cluster
        task.update({"new_cluster": definition})

    def process(self) -> Dict[str, Any]:
        with Console().status("🔍 Iterating over task definitions to find shared job cluster usages", spinner="dots"):
            for task in self._job_spec.get("tasks", []):
                if "job_cluster_key" in task:
                    self._preprocess_task_definition(task)
        dbx_echo("✅ All shared job cluster usages were replaced with their relevant cluster definitions")
        return self._job_spec
