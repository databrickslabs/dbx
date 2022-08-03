from abc import ABC, abstractmethod
from pathlib import Path, PurePosixPath
from typing import Optional, Dict

import mlflow
from retry import retry

from dbx.api.configure import ConfigurationManager
from dbx.api.context import RichExecutionContextClient
from dbx.utils import dbx_echo


class AbstractFileUploader(ABC):
    def __init__(self, base_uri: Optional[str] = None):
        self._base_uri = base_uri
        self._uploaded_files: Dict[Path, str] = {}  # contains mapping from local to remote paths for all uploaded files

    @abstractmethod
    def _upload_file(self, local_file_path: Path):
        """"""

    def _verify_fuse_support(self):
        if not self._base_uri.startswith("dbfs:/"):
            raise Exception(
                "Fuse-based paths are not supported for non-dbfs artifact locations."
                "If fuse-like paths are required, consider using experiment with DBFS as a location."
            )

    def upload_and_provide_path(self, local_file_path: Path, as_fuse: Optional[bool] = False) -> str:
        if as_fuse:
            self._verify_fuse_support()

        if local_file_path in self._uploaded_files:
            remote_path = self._uploaded_files[local_file_path]
        else:
            self._upload_file(local_file_path)
            remote_path = "/".join([self._base_uri, str(local_file_path.as_posix())])
            self._uploaded_files[local_file_path] = remote_path

        remote_path = remote_path.replace("dbfs:/", "/dbfs/") if as_fuse else remote_path
        return remote_path


class MlflowFileUploader(AbstractFileUploader):
    """
    MlflowFileUploader represents a class that is used for uploading local files into mlflow storage
    """

    @staticmethod
    @retry(tries=3, delay=1, backoff=0.3)
    def _upload_file(file_path: Path):
        dbx_echo(f"Uploading file {file_path}")
        posix_path = PurePosixPath(file_path.as_posix())
        parent = str(posix_path.parent) if str(posix_path.parent) != "." else None
        dbx_echo(f"Uploading file {file_path} - done")
        mlflow.log_artifact(str(file_path), parent)


class ContextBasedUploader(AbstractFileUploader):
    def __init__(self, client: RichExecutionContextClient):
        self._client = client
        temp_dir = self._client.get_temp_dir()
        super().__init__(base_uri=str(temp_dir))

    def _verify_fuse_support(self):
        dbx_echo("Skipping the FUSE check since context-based uploader is used")

    @staticmethod
    def _get_current_experiment_id(env_name: str) -> str:
        info = ConfigurationManager().get(env_name)
        return mlflow.get_experiment_by_name(info.workspace_dir).experiment_id

    def _upload_file(self, local_file_path: Path):
        temp_remote_file_path = self._client.upload_file(local_file_path, Path(self._base_uri))
        dbx_echo(f"File uploaded to temp location {temp_remote_file_path}")

    def __del__(self):
        try:
            dbx_echo("Cleaning up the temp directory")
            self._client.remove_dir(self._base_uri)
            dbx_echo("Cleaning up the temp directory - done")
        except Exception as e:
            dbx_echo(f"Cannot cleanup temp directory due to {e}")
