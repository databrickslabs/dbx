import pathlib
from typing import List, Dict, Any

from databricks_cli.sdk import ApiClient

from dbx.utils import dbx_echo
from dbx.utils.file_uploader import MlflowFileUploader, AbstractFileUploader
from dbx.utils.dependency_manager import DependencyManager
from dbx.utils.named_properties import WorkloadPropertiesProcessor, NewClusterPropertiesProcessor, PolicyNameProcessor


def adjust_job_definitions(
    jobs: List[Dict[str, Any]],
    dependency_manager: DependencyManager,
    file_uploader: MlflowFileUploader,
    api_client: ApiClient,
):
    def adjustment_callback(p: Any):
        return adjust_path(p, file_uploader)

    for job in jobs:

        # please note that all adjustments here have side effects to the main jobs object.

        workload_processor = WorkloadPropertiesProcessor(api_client)
        new_cluster_processor = NewClusterPropertiesProcessor(api_client)
        policy_name_processor = PolicyNameProcessor(api_client)

        adjustable_references = []

        if "tasks" in job:
            dbx_echo(f"Tasks section found in the job {job['name']}, job will be deployed as a multitask job")
            adjustable_references += job["tasks"]
            job_clusters = job.get("job_clusters", [])
            for jc_reference in job_clusters:
                cluster_definition = jc_reference.get("new_cluster", {})
                policy_name_processor.process(cluster_definition)
                new_cluster_processor.process(cluster_definition)
        else:
            dbx_echo(f"Tasks section not found in the job {job['name']}, job will be deployed as a single-task job")
            adjustable_references.append(job)

        for workload_reference in adjustable_references:

            dependency_manager.process_dependencies(workload_reference)
            workload_processor.process(workload_reference)

            new_cluster_definition = workload_reference.get("new_cluster", {})

            if new_cluster_definition:
                policy_name_processor.process(new_cluster_definition)
                new_cluster_processor.process(new_cluster_definition)

            walk_content(adjustment_callback, workload_reference)


def walk_content(func, content, parent=None, index=None):
    if isinstance(content, dict):
        for key, item in content.items():
            walk_content(func, item, content, key)
    elif isinstance(content, list):
        for idx, sub_item in enumerate(content):
            walk_content(func, sub_item, content, idx)
    else:
        parent[index] = func(content)


def path_adjustment(candidate: str, file_uploader: AbstractFileUploader) -> str:
    if candidate.startswith("file:"):
        fuse_flag = candidate.startswith("file:fuse:")
        replace_string = "file:fuse://" if fuse_flag else "file://"
        local_path = pathlib.Path(candidate.replace(replace_string, ""))

        if not local_path.exists():
            raise FileNotFoundError(
                f"Path {candidate} is referenced in the deployment configuration, but is non-existent."
            )

        adjusted_path = file_uploader.upload_and_provide_path(local_path, as_fuse=fuse_flag)

        return adjusted_path

    else:
        return candidate


def adjust_path(candidate, file_uploader: AbstractFileUploader):
    if isinstance(candidate, str):
        # path already adjusted or points to another dbfs object - pass it
        if candidate.startswith("dbfs") or candidate.startswith("/dbfs"):
            return candidate
        else:
            adjusted_path = path_adjustment(candidate, file_uploader)
            return adjusted_path
    else:
        return candidate
