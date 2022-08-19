from typing import Dict, Any

from databricks_cli.sdk import ApiClient

from dbx.api.launch.functions import trace_run, cancel_run
from dbx.utils import dbx_echo


class RunTracer:
    @staticmethod
    def start(kill_on_sigterm: bool, api_client: ApiClient, run_data: Dict[str, Any]):
        if kill_on_sigterm:
            dbx_echo("Click Ctrl+C to stop the run")
            try:
                dbx_status, final_run_state = trace_run(api_client, run_data)
            except KeyboardInterrupt:
                dbx_status = "CANCELLED"
                final_run_state = {}
                dbx_echo("Cancelling the run gracefully")
                cancel_run(api_client, run_data)
                dbx_echo("Run cancelled successfully")
        else:
            dbx_status, final_run_state = trace_run(api_client, run_data)

        return dbx_status, final_run_state
