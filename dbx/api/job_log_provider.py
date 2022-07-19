from typing import Dict, Any

from databricks_cli.sdk import JobsService

from dbx.utils import dbx_echo


class JobLogProvider:
    def __init__(self, js: JobsService, job_run_status: Dict[str, Any]):
        self._js = js
        self._status = job_run_status

    def get_all(self):


