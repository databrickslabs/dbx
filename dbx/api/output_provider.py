from typing import Dict, Any, List

from databricks_cli.sdk import JobsService

from dbx.utils import dbx_echo


class OutputProvider:
    def __init__(self, js: JobsService, final_state: Dict[str, Any]):
        self._js = js
        self._final_state = final_state

    @staticmethod
    def _print_by_key(output: Dict[str, Any], key: str):
        logs = output.get(key, "")
        for line in logs.split("\n"):
            dbx_echo(line)

    @staticmethod
    def _wrap_message(msg):
        return "-" * 20 + f" {msg} " + "-" * 20

    def provide(self, output_level: str):
        tasks: List[Any] = self._final_state.get("tasks", [])
        tasks.sort(key=lambda el: el["run_id"])  # sort is in-place function,

        if not tasks:
            dbx_echo("Output cannot be captured since the job is not based on Jobs API V2.X+")
        else:
            for task in tasks:
                run_id = task["run_id"]
                task_key = task["task_key"]
                output = self._js.get_run_output(run_id)
                dbx_echo(f"Output for the task {task_key} from run {run_id}")

                if output_level == "stdout":
                    if output.get("logs"):
                        dbx_echo(self._wrap_message("stdout"))
                        self._print_by_key(output, "logs")
                        dbx_echo(self._wrap_message("stdout end"))
                    else:
                        dbx_echo("No stdout provided in the run output")

                if output.get("error"):
                    dbx_echo("stderr is not empty for the given task, please find the run error output below")
                    dbx_echo(self._wrap_message("stderr"))
                    self._print_by_key(output, "error")
                    dbx_echo(self._wrap_message("stderr end"))

                if output.get("error_trace"):
                    dbx_echo(self._wrap_message("error trace"))
                    self._print_by_key(output, "error_trace")
                    dbx_echo(self._wrap_message("error trace end"))
