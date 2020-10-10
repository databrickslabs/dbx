import asyncio

from databricks_cli.sdk import ApiClient, ClusterService


class ClusterManager:
    def __init__(self, api_client: ApiClient, cluster_id: str):
        self._api_client = api_client
        self._cluster_service = ClusterService(api_client)
        self.cluster_id = cluster_id
        self._status = "initializing"

    @property
    def status(self):
        return self._status.lower()

    async def cluster_routine(self):
        while True:
            cluster_status = self._cluster_service.get_cluster(self.cluster_id).get("state")
            self._status = cluster_status
            if cluster_status in ["RUNNING", "RESIZING"]:
                await asyncio.sleep(5)
            if cluster_status in ["TERMINATED", "TERMINATING"]:
                self._cluster_service.start_cluster(self.cluster_id)
                await asyncio.sleep(5)
            elif cluster_status == "ERROR":
                raise Exception(
                    "Cluster is mis-configured and cannot be started, please check cluster settings at first")
            elif cluster_status in ["PENDING", "RESTARTING"]:
                await asyncio.sleep(10)
