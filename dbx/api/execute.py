from pathlib import Path
from typing import Optional, List, Any

import mlflow

from dbx.api.context import RichExecutionContextClient
from dbx.utils import dbx_echo
from dbx.utils.adjuster import adjust_path, walk_content
from dbx.utils.common import get_package_file
from dbx.utils.file_uploader import MlflowFileUploader, ContextBasedUploader


class ExecutionController:
    def __init__(
        self,
        client: RichExecutionContextClient,
        entrypoint_file: Path,
        no_package: bool,
        upload_via_context: bool,
        requirements_file: Optional[Path],
        task_parameters: Optional[List[str]],
    ):
        self._client = client
        self._requirements_file = requirements_file
        self._no_package = no_package
        self._task_parameters = task_parameters
        self._entrypoint_file = entrypoint_file
        self._upload_via_context = upload_via_context

        self._run = None

        if not self._upload_via_context:
            self._run = mlflow.start_run()
            self._file_uploader = MlflowFileUploader(self._run.info.artifact_uri)
        else:
            self._file_uploader = ContextBasedUploader(self._client)

    def execute_entrypoint_file(self):
        dbx_echo("Starting entrypoint file execution")
        self._client.execute_file(self._entrypoint_file)
        dbx_echo("Command execution finished")

    def run(self):
        self.install_requirements_file()
        if not self._no_package:
            self.install_package()
        if self._task_parameters:
            self.preprocess_task_parameters()
        self.execute_entrypoint_file()
        if self._run:
            mlflow.end_run()

    def install_requirements_file(self):
        if self._requirements_file.exists():
            dbx_echo("Installing provided requirements")
            localized_requirements_path = self._file_uploader.upload_and_provide_path(
                self._requirements_file, as_fuse=True
            )
            installation_command = f"%pip install -U -r {localized_requirements_path}"
            self._client.client.execute_command(installation_command, verbose=False)
            dbx_echo("Provided requirements installed")

    def install_package(self):
        package_file = get_package_file()
        if not package_file:
            raise FileNotFoundError("Project package was not found. Please check that /dist directory exists.")
        dbx_echo("Installing package")
        driver_package_path = self._file_uploader.upload_and_provide_path(package_file, as_fuse=True)
        self._client.install_package(Path(driver_package_path))
        dbx_echo("Package installation finished")

    def preprocess_task_parameters(self):
        dbx_echo(f"Processing task parameters: {self._task_parameters}")

        def adjustment_callback(p: Any):
            return adjust_path(p, self._file_uploader)

        walk_content(adjustment_callback, self._task_parameters)

        self._client.setup_arguments(self._task_parameters)
        dbx_echo("Processing task parameters - done")
