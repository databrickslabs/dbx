from typing import List, Dict, Any, Optional

from databricks_cli.sdk.service import JobsService


def list_all_jobs(js: JobsService) -> List[Dict[str, Any]]:
    all_jobs = js.list_jobs(
        version="2.0"
    )  # version 2.0 is expected to list all jobs without iterations over limit/offset
    return all_jobs.get("jobs", [])


def find_job_by_name(js: JobsService, job_name: str) -> Optional[Dict[str, Any]]:
    all_jobs = list_all_jobs(js)
    matching_jobs = [j for j in all_jobs if j["settings"]["name"] == job_name]

    if len(matching_jobs) > 1:
        raise Exception(
            f"""There are more than one jobs with name {job_name}.
                Please delete duplicated jobs first"""
        )

    if not matching_jobs:
        return None
    else:
        return matching_jobs[0]
