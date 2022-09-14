from typing import Dict, Any, List

from pydantic import BaseModel, root_validator


class JobCluster(BaseModel):
    job_cluster_key: str
    new_cluster: Dict[str, Any]


class JobClusters(BaseModel):
    job_clusters: List[JobCluster] = []

    @root_validator(pre=True)
    def validator(cls, values: Dict[str, Any]):  # noqa
        if values:
            job_clusters = values.get("job_clusters", [])

            # checks that structure is provided in expected format
            assert isinstance(job_clusters, list), f"Job clusters payload should be a list, provided: {job_clusters}"

            cluster_keys = [JobCluster(**v).job_cluster_key for v in job_clusters]

            # checks that there are no duplicates
            for key in cluster_keys:
                if cluster_keys.count(key) > 1:
                    raise ValueError(f"Duplicated cluster key {key} found in the job_clusters section")
        return values

    def get_cluster_definition(self, key: str) -> JobCluster:
        _found = list(filter(lambda jc: jc.job_cluster_key == key, self.job_clusters))
        if not _found:
            raise ValueError(f"Cluster key {key} is not provided in the job_clusters section: {self.job_clusters}")
        return _found[0]
