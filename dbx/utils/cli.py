from typing import List, Dict, Any, Optional

from databricks_cli.sdk import ClusterService

from dbx.api.clients.databricks_api import DatabricksClientProvider


def parse_list_of_arguments(multiple_arguments: Optional[List[str]] = None) -> Dict[str, Any]:
    _tags = [t.split("=") for t in multiple_arguments] if multiple_arguments else []
    _tags = {t[0]: t[1] for t in _tags}
    return _tags


class ClusterArgumentPreprocessor:
    @staticmethod
    def preprocess(cluster_name: Optional[str], cluster_id: Optional[str]) -> str:
        api_client = DatabricksClientProvider.get_v2_client()
        cluster_service = ClusterService(api_client)

        if not cluster_name and not cluster_id:
            raise RuntimeError("Parameters --cluster-name and --cluster-id couldn't be empty at the same time.")

        if cluster_name:

            existing_clusters = [
                c
                for c in cluster_service.list_clusters().get("clusters")
                if not c.get("cluster_name").startswith("job-")
            ]
            matching_clusters = [c for c in existing_clusters if c.get("cluster_name") == cluster_name]

            if not matching_clusters:
                cluster_names = [c["cluster_name"] for c in existing_clusters]
                raise NameError(f"No clusters with name {cluster_name} found. Available clusters are: {cluster_names}")
            if len(matching_clusters) > 1:
                raise NameError(f"Found more then one cluster with name {cluster_name}: {matching_clusters}")

            cluster_id = matching_clusters[0]["cluster_id"]
        else:
            if not cluster_service.get_cluster(cluster_id):
                raise NameError(f"Cluster with id {cluster_id} not found")

        return cluster_id
