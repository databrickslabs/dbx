import time
from pathlib import PosixPath, Path
from typing import Optional, List, Any

from databricks_cli.sdk import ApiClient

from dbx.api.client_provider import ApiV1Client
from dbx.api.context import LocalContextManager
from dbx.utils import dbx_echo


class ExecutionContextClient:
    def __init__(self, v2_client: ApiClient, cluster_id: str, language: str = "python"):
        self._v1_client = ApiV1Client(v2_client)
        self._cluster_id = cluster_id
        self._context_id = self.__get_context_id(language)

    def _wait_for_command_execution(self, command_id: str):
        finished = False
        payload = {
            "clusterId": self._cluster_id,
            "contextId": self._context_id,
            "commandId": command_id,
        }
        while not finished:
            try:
                result = self._v1_client.get_command_status(payload)
                status = result.get("status")
                if status in ["Finished", "Cancelled", "Error"]:
                    return result
                else:
                    time.sleep(5)
            except KeyboardInterrupt:
                self._v1_client.cancel_command(payload)

    def execute_command(self, command: str, verbose=True) -> Optional[str]:
        payload = {
            "language": "python",
            "clusterId": self._cluster_id,
            "contextId": self._context_id,
            "command": command,
        }
        command_execution_data = self._v1_client.execute_command(payload)
        command_id = command_execution_data["id"]
        execution_result = self._wait_for_command_execution(command_id)
        result_data = execution_result["results"].get("data")

        if execution_result["status"] == "Cancelled":
            dbx_echo("Command cancelled")
        else:
            final_result = execution_result["results"]["resultType"]
            if final_result == "error":
                dbx_echo("Execution failed, please follow the given error")
                raise RuntimeError(
                    "Command execution failed. Traceback from cluster: \n" f'{execution_result["results"]["cause"]}'
                )

            if verbose:
                dbx_echo("Command successfully executed")
                if result_data:
                    print(result_data)

            return result_data

    def __is_context_available(self, context_id: str):
        if not context_id:
            return False
        else:
            payload = {"clusterId": self._cluster_id, "contextId": context_id}
            resp = self._v1_client.get_context_status(payload)
            if not resp:
                return False
            elif resp.get("status"):
                return resp["status"] == "Running"

    def __get_context_id(self, language: str):
        dbx_echo("Preparing execution context")
        lock_context_id = LocalContextManager.get_context()

        if self.__is_context_available(lock_context_id):
            dbx_echo("Existing context is active, using it")
            return lock_context_id
        else:
            dbx_echo("Existing context is not active, creating a new one")
            context_id = self.__create_context(language)
            LocalContextManager.set_context(context_id)
            dbx_echo("New context prepared, ready to use it")
            return context_id

    def __create_context(self, language: str):
        payload = {"language": language, "clusterId": self._cluster_id}
        response = self._v1_client.create_context(payload)
        return response["id"]

    @property
    def context_id(self):
        return self._context_id


class RichExecutionContextClient:
    def __init__(self, v2_client: ApiClient, cluster_id: str, language: str = "python"):
        self._client = ExecutionContextClient(v2_client, cluster_id, language)

    def get_context_id(self) -> str:
        return self._client.context_id

    def get_workspace_id(self) -> str:
        workspace_id_command = """
        import json
        org_id = json.loads(
            dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
        )["tags"]["orgId"]
        print(org_id)
        """
        response = self._client.execute_command(workspace_id_command, verbose=False)
        if not response:
            raise RuntimeError(
                "While running the command for workspace ID exploration, empty response was returned. "
                "Please create a new GitHub issue."
            )
        return response

    def install_package(self, package_file: PosixPath):
        installation_command = f"%pip install --force-reinstall {package_file.absolute()}"
        self._client.execute_command(installation_command, verbose=False)

    def setup_arguments(self, arguments: List[Any]):
        task_props = ["python"] + [str(arg) for arg in arguments]
        setup_command = f"""
        import sys
        sys.argv = {task_props}
        """
        self._client.execute_command(setup_command, verbose=False)

    def execute_file(self, file_path: Path):
        content = file_path.read_text(encoding="utf-8")
        self._client.execute_command(content, verbose=True)
