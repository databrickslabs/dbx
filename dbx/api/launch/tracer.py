import time

from databricks_cli.sdk import ApiClient
from rich.console import Console

from dbx.api.launch.functions import trace_run, cancel_run
from dbx.api.launch.pipeline_models import PipelineUpdateState, PipelineUpdateStatus
from dbx.api.launch.runners.base import RunData, PipelineUpdateResponse
from dbx.utils import dbx_echo


class RunTracer:
    @staticmethod
    def start(kill_on_sigterm: bool, api_client: ApiClient, run_data: RunData):
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


class PipelineTracer:
    TERMINAL_STATES = [PipelineUpdateState.COMPLETED, PipelineUpdateState.FAILED, PipelineUpdateState.CANCELED]

    @classmethod
    def start(
        cls, api_client: ApiClient, process_info: PipelineUpdateResponse, pipeline_id: str
    ) -> PipelineUpdateState:
        _path = f"/pipelines/{pipeline_id}/requests/{process_info.request_id}"
        with Console().status(f"Tracing the DLT pipeline with id {pipeline_id}", spinner="dots") as display:
            while True:
                time.sleep(1)  # to avoid API throttling
                raw_response = api_client.perform_query("GET", _path)
                status_response = PipelineUpdateStatus(**raw_response)
                latest_update = status_response.latest_update

                msg = (
                    f"Tracing the DLT pipeline with id {pipeline_id}, "
                    f"started with cause {latest_update.cause}, "
                    f"current state: {latest_update.state}"
                )

                display.update(msg)
                if latest_update.state in cls.TERMINAL_STATES:
                    display.update(f"DLT pipeline with id {pipeline_id} finished with state {latest_update.state}")
                    return latest_update.state
