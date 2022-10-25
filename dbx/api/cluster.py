import time
from typing import Optional

from databricks_cli.clusters.api import ClusterService
from databricks_cli.sdk import ApiClient
from rich.console import Console
from rich.status import Status


class ClusterController:
    def __init__(self, api_client: ApiClient, cluster_name: Optional[str], cluster_id: Optional[str]):
        self._cluster_service = ClusterService(api_client)
        self.cluster_id = self.preprocess_cluster_args(cluster_name, cluster_id)

    def awake_cluster(self):
        with Console().status("Preparing the all-purpose cluster to accept commands", spinner="dots") as status:
            self._awake_cluster(self.cluster_id, status)

    def _awake_cluster(self, cluster_id, status: Status):
        cluster_info = self._cluster_service.get_cluster(cluster_id)
        if cluster_info["state"] in ["RUNNING", "RESIZING"]:
            status.update("Cluster is ready")
        if cluster_info["state"] in ["TERMINATED", "TERMINATING"]:
            status.update("Dev cluster is terminated, starting it")
            self._cluster_service.start_cluster(cluster_id)
            time.sleep(5)
            self._awake_cluster(cluster_id, status)
        elif cluster_info["state"] == "ERROR":
            raise RuntimeError(
                "Cluster is mis-configured and cannot be started, please check cluster settings at first"
            )
        elif cluster_info["state"] in ["PENDING", "RESTARTING"]:
            status.update(f'Cluster is getting prepared, current state: {cluster_info["state"]}')
            time.sleep(5)
            self._awake_cluster(cluster_id, status)

    def preprocess_cluster_args(self, cluster_name: Optional[str], cluster_id: Optional[str]) -> str:

        if not cluster_name and not cluster_id:
            raise RuntimeError("Parameters --cluster-name and --cluster-id couldn't be empty at the same time.")

        if cluster_name:

            existing_clusters = [
                c
                for c in self._cluster_service.list_clusters().get("clusters")
                if not c.get("cluster_name").startswith("job-")
            ]
            matching_clusters = [c for c in existing_clusters if c.get("cluster_name") == cluster_name]

            if not matching_clusters:
                cluster_names = [c["cluster_name"] for c in existing_clusters]
                raise NameError(f"No clusters with name {cluster_name} found. Available clusters are: {cluster_names} ")
            if len(matching_clusters) > 1:
                raise NameError(f"Found more then one cluster with name {cluster_name}: {matching_clusters}")

            cluster_id = matching_clusters[0]["cluster_id"]
        else:
            if not self._cluster_service.get_cluster(cluster_id):
                raise NameError(f"Cluster with id {cluster_id} not found")

        return cluster_id
