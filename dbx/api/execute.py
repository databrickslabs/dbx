from pathlib import Path
from typing import Optional, List, Any

import mlflow

from dbx.api.context import RichExecutionContextClient
from dbx.models.task import Task, TaskType, PythonWheelTask
from dbx.utils import dbx_echo
from dbx.utils.adjuster import adjust_path, walk_content
from dbx.utils.common import get_package_file
from dbx.utils.file_uploader import MlflowFileUploader, ContextBasedUploader


class ExecutionController:
    def __init__(
        self,
        client: RichExecutionContextClient,
        no_package: bool,
        upload_via_context: bool,
        requirements_file: Optional[Path],
        task: Task,
    ):
        self._client = client
        self._requirements_file = requirements_file
        self._no_package = no_package
        self._task = task
        self._upload_via_context = upload_via_context

        self._run = None

        if not self._upload_via_context:
            self._run = mlflow.start_run()
            self._file_uploader = MlflowFileUploader(self._run.info.artifact_uri)
        else:
            self._file_uploader = ContextBasedUploader(self._client)

    def execute_entrypoint_file(self, _file: Path):
        dbx_echo("Starting entrypoint file execution")
        self._client.execute_file(_file)
        dbx_echo("Command execution finished")

    def execute_entrypoint(self, task: PythonWheelTask):
        dbx_echo("Starting entrypoint execution")
        self._client.execute_entry_point(task.package_name, task.entry_point)
        dbx_echo("Entrypoint execution finished")

    def run(self):
        if self._requirements_file.exists():
            self.install_requirements_file()

        if not self._no_package:
            self.install_package()

        if self._task.task_type == TaskType.spark_python_task:
            self.preprocess_task_parameters(self._task.spark_python_task.parameters)
            self.execute_entrypoint_file(self._task.spark_python_task.python_file)
        elif self._task.task_type == TaskType.python_wheel_task:
            if self._task.python_wheel_task.named_parameters:
                self.preprocess_task_parameters(self._task.python_wheel_task.named_parameters)
            elif self._task.python_wheel_task.parameters:
                self.preprocess_task_parameters(self._task.python_wheel_task.parameters)
            self.execute_entrypoint(self._task.python_wheel_task)

        if self._run:
            mlflow.end_run()

    def install_requirements_file(self):
        dbx_echo("Installing provided requirements")
        localized_requirements_path = self._file_uploader.upload_and_provide_path(self._requirements_file, as_fuse=True)
        installation_command = f"%pip install -U -r {localized_requirements_path}"
        self._client.client.execute_command(installation_command, verbose=False)
        dbx_echo("Provided requirements installed")

    def install_package(self):
        package_file = get_package_file()
        if not package_file:
            raise FileNotFoundError("Project package was not found. Please check that /dist directory exists.")
        dbx_echo("Uploading package")
        driver_package_path = self._file_uploader.upload_and_provide_path(package_file, as_fuse=True)
        dbx_echo(":white_check_mark: Uploading package - done")
        dbx_echo("Installing package")
        self._client.install_package(driver_package_path)
        dbx_echo(":white_check_mark: Installing package - done")

    def preprocess_task_parameters(self, parameters: List[str]):
        dbx_echo(f":fast_forward: Processing task parameters: {parameters}")

        def adjustment_callback(p: Any):
            return adjust_path(p, self._file_uploader)

        walk_content(adjustment_callback, parameters)

        self._client.setup_arguments(parameters)
        dbx_echo(":white_check_mark: Processing task parameters")
