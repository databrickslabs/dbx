import time
from base64 import b64encode
from pathlib import Path
from typing import Optional, List, Any

import typer
from databricks_cli.sdk import ApiClient

from dbx.api.client_provider import ApiV1Client
from dbx.constants import LOCK_FILE_PATH
from dbx.models.files.context import ContextInfo
from dbx.utils import dbx_echo
from dbx.utils.json import JsonUtils


class LocalContextManager:
    context_file_path: Path = LOCK_FILE_PATH

    @classmethod
    def set_context(cls, ctx: ContextInfo) -> None:
        JsonUtils.write(cls.context_file_path, ctx.dict())

    @classmethod
    def get_context(cls) -> Optional[ContextInfo]:
        if cls.context_file_path.exists():
            return ContextInfo(**JsonUtils.read(cls.context_file_path))
        else:
            return None


class LowLevelExecutionContextClient:
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
                _traceback = execution_result["results"]["cause"]
                print(_traceback)
                raise typer.Exit(1)

            if verbose:
                dbx_echo("Command successfully executed")
                if result_data:
                    dbx_echo("🔊 stdout from the execution is shown below:")
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

    def __get_context_id(self, language: str) -> str:
        dbx_echo("Preparing execution context")
        ctx = LocalContextManager.get_context()

        if ctx and self.__is_context_available(ctx.context_id):
            dbx_echo("Existing context is active, using it")
            return ctx.context_id
        else:
            dbx_echo("Existing context is not active, creating a new one")
            context_id = self.__create_context(language)
            # we add additional str conversion here to make mocks in test work
            LocalContextManager.set_context(ContextInfo(context_id=str(context_id)))
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
        self.api_client = v2_client
        self._client = LowLevelExecutionContextClient(v2_client, cluster_id, language)

    def install_package(self, package_file: str, pip_install_extras: Optional[str]):
        if pip_install_extras:
            installation_command = f'%pip install --force-reinstall "{package_file}[{pip_install_extras}]"'
        else:
            installation_command = f"%pip install --force-reinstall {package_file}"
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

    def execute_entry_point(self, package_name: str, entry_point: str):
        command = f"""
        import pkg_resources
        _func = pkg_resources.load_entry_point("{package_name}", "console_scripts", "{entry_point}")
        _func()
        """
        self._client.execute_command(command)

    @property
    def client(self):
        return self._client

    def get_temp_dir(self) -> str:
        command = """
        from tempfile import mkdtemp
        print(mkdtemp())
        """
        return self._client.execute_command(command, verbose=False)

    def remove_dir(self, _dir: str):
        command = f"""
        import shutil
        shutil.rmtree("{_dir}")
        """
        self._client.execute_command(command, verbose=False)

    def upload_file(self, file: Path, prefix_dir: str) -> str:
        _contents = file.read_bytes()
        contents = b64encode(_contents)
        command = f"""
        from pathlib import Path
        from base64 import b64decode
        DBX_UPLOAD_CONTENTS = b64decode({contents})
        file_path = Path("{prefix_dir}") / "{file.as_posix()}"
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True)
        file_path.write_bytes(DBX_UPLOAD_CONTENTS)
        print(file_path)
        """
        return self._client.execute_command(command, verbose=False)
