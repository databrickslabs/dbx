from typing import Dict, Any

from databricks_cli.sdk import JobsService

from dbx.utils import dbx_echo
from dbx.utils.output_wrapper import OutputWrapper


class LogProvider:
    def __init__(self, js: JobsService, final_state: Dict[str, Any]):
        self._js = js
        self._final_state = final_state

    @staticmethod
    def _print_by_key(output: Dict[str, Any], key: str):
        with OutputWrapper(symbol="#"):
            logs = output.get(key)
            if logs:
                for line in logs.split("\n"):
                    dbx_echo(line)

    def provide(self, log_level: str):
        tasks = self._final_state.get("tasks")
        if not tasks:
            dbx_echo("Logs cannot pre collected since the job is not based on Jobs API V2.X+")
        else:
            for task in tasks:
                run_id = task["run_id"]
                task_key = task["task_key"]
                output = self._js.get_run_output(run_id)
                dbx_echo(f"Logs for the task {task_key} from run {run_id}")

                if log_level == "all":
                    self._print_by_key(output, "logs")

                if output.get("error"):
                    dbx_echo("Error message is not empty for the given task, please find the job error output below")
                    self._print_by_key(output, "error")
