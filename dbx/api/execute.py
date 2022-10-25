from pathlib import Path
from typing import Optional, List

import mlflow
from rich.console import Console

from dbx.api.adjuster.adjuster import Adjuster, AdditionalLibrariesProvider
from dbx.api.context import RichExecutionContextClient
from dbx.models.workflow.common.libraries import Library
from dbx.models.workflow.common.task_type import TaskType
from dbx.models.workflow.v2dot1.task import PythonWheelTask
from dbx.types import ExecuteTask
from dbx.utils import dbx_echo
from dbx.utils.file_uploader import MlflowFileUploader, ContextBasedUploader


class ExecutionController:
    def __init__(
        self,
        client: RichExecutionContextClient,
        no_package: bool,
        core_package: Optional[Library],
        upload_via_context: bool,
        requirements_file: Optional[Path],
        task: ExecuteTask,
        pip_install_extras: Optional[str],
    ):
        self.additional_libraries = AdditionalLibrariesProvider(no_package=no_package, core_package=core_package)
        self._client = client
        self._requirements_file = requirements_file
        self._task = task
        self._upload_via_context = upload_via_context
        self._pip_install_extras = pip_install_extras
        self._run = None

        if self._upload_via_context:
            dbx_echo("Context-based file uploader will be used")
            self._file_uploader = ContextBasedUploader(self._client)
        else:
            dbx_echo("Mlflow-based file uploader will be used")
            self._run = mlflow.start_run()
            self._file_uploader = MlflowFileUploader(self._run.info.artifact_uri)

    def execute_entrypoint_file(self, _file: Path):
        dbx_echo("Starting entrypoint file execution")
        with Console().status("Running the entrypoint file", spinner="dots"):
            self._client.execute_file(_file)
        dbx_echo("Command execution finished")

    def execute_entrypoint(self, task: PythonWheelTask):
        dbx_echo("Starting entrypoint execution")
        with Console().status("Running the entrypoint", spinner="dots"):
            self._client.execute_entry_point(task.package_name, task.entry_point)
        dbx_echo("Entrypoint execution finished")

    def run(self):
        if self._requirements_file:
            self.install_requirements_file()

        if not self.additional_libraries.no_package:
            self.install_package(self._pip_install_extras)

        if self._task.task_type == TaskType.spark_python_task:
            self.preprocess_task_parameters(self._task.spark_python_task.parameters)
            self.execute_entrypoint_file(self._task.spark_python_task.execute_file)

        elif self._task.task_type == TaskType.python_wheel_task:
            if self._task.python_wheel_task.named_parameters:
                self.preprocess_task_parameters(self._task.python_wheel_task.named_parameters)
            elif self._task.python_wheel_task.parameters:
                self.preprocess_task_parameters(self._task.python_wheel_task.parameters)
            self.execute_entrypoint(self._task.python_wheel_task)

        if self._run:
            mlflow.end_run()

    def install_requirements_file(self):
        if not self._requirements_file.exists():
            raise Exception(f"Requirements file provided, but doesn't exist at path {self._requirements_file}")

        dbx_echo("Installing provided requirements")
        localized_requirements_path = self._file_uploader.upload_and_provide_path(
            f"file:fuse://{self._requirements_file}"
        )
        installation_command = f"%pip install -U -r {localized_requirements_path}"
        self._client.client.execute_command(installation_command, verbose=False)
        dbx_echo("Provided requirements installed")

    def install_package(self, pip_install_extras: Optional[str]):
        if not self.additional_libraries.core_package:
            raise FileNotFoundError("Project package was not found. Please check that /dist directory exists.")
        dbx_echo("Uploading package")
        stripped_package_path = self.additional_libraries.core_package.whl.replace("file://", "")
        localized_package_path = self._file_uploader.upload_and_provide_path(f"file:fuse://{stripped_package_path}")
        dbx_echo(":white_check_mark: Uploading package - done")

        with Console().status("Installing package on the cluster 📦", spinner="dots"):
            self._client.install_package(localized_package_path, pip_install_extras)

        dbx_echo(":white_check_mark: Installing package - done")

    def preprocess_task_parameters(self, parameters: List[str]):
        dbx_echo(f":fast_forward: Processing task parameters: {parameters}")

        Adjuster(
            api_client=self._client.api_client,
            additional_libraries=self.additional_libraries,
            file_uploader=self._file_uploader,
        ).traverse(parameters)

        self._client.setup_arguments(parameters)
        dbx_echo(":white_check_mark: Processing task parameters")
