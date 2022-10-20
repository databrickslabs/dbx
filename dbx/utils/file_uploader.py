import functools
from abc import ABC, abstractmethod
from pathlib import Path, PurePosixPath
from typing import Optional, Tuple

import mlflow
from retry import retry

from dbx.api.context import RichExecutionContextClient
from dbx.utils import dbx_echo


class AbstractFileUploader(ABC):
    def __init__(self, base_uri: Optional[str] = None):
        self.base_uri = base_uri

    @abstractmethod
    def _upload_file(self, local_file_path: Path):
        """"""

    def _verify_fuse_support(self):
        if not self.base_uri.startswith("dbfs:/"):
            raise Exception(
                "Fuse-based paths are not supported for non-dbfs artifact locations."
                "If fuse-like paths are required, consider using experiment with DBFS as a location."
            )

    def _postprocess_path(self, local_file_path: Path, as_fuse) -> str:
        remote_path = "/".join([self.base_uri, str(local_file_path.as_posix())])
        remote_path = remote_path.replace("dbfs:/", "/dbfs/") if as_fuse else remote_path

        if self.base_uri.startswith("wasbs://"):
            remote_path = remote_path.replace("wasbs://", "abfss://")
            remote_path = remote_path.replace(".blob.", ".dfs.")

        return remote_path

    @staticmethod
    def _preprocess_reference(ref: str) -> Tuple[Path, bool]:
        _as_fuse = ref.startswith("file:fuse://")
        _corrected = ref.replace("file:fuse://", "") if _as_fuse else ref.replace("file://", "")
        _path = Path(_corrected)
        return _path, _as_fuse

    @staticmethod
    def _verify_reference(ref, _path: Path):
        if not _path.exists():
            raise FileNotFoundError(f"Provided file reference: {ref} doesn't exist in the local FS")

    @functools.lru_cache(maxsize=3000)
    def upload_and_provide_path(self, file_reference: str) -> str:
        local_file_path, as_fuse = self._preprocess_reference(file_reference)
        self._verify_reference(file_reference, local_file_path)

        if as_fuse:
            self._verify_fuse_support()

        dbx_echo(f":arrow_up: Uploading local file {local_file_path}")
        self._upload_file(local_file_path)
        dbx_echo(f":white_check_mark: Uploading local file {local_file_path}")
        return self._postprocess_path(local_file_path, as_fuse)


class MlflowFileUploader(AbstractFileUploader):
    """
    MlflowFileUploader represents a class that is used for uploading local files into mlflow storage
    """

    @staticmethod
    @retry(tries=3, delay=1, backoff=0.3)
    def _upload_file(file_path: Path):
        posix_path = PurePosixPath(file_path.as_posix())
        parent = str(posix_path.parent) if str(posix_path.parent) != "." else None
        mlflow.log_artifact(str(file_path), parent)


class ContextBasedUploader(AbstractFileUploader):
    def __init__(self, client: RichExecutionContextClient):
        self._client = client
        temp_dir = self._client.get_temp_dir()
        super().__init__(base_uri=temp_dir)

    def _verify_fuse_support(self):
        dbx_echo("Skipping the FUSE check since context-based uploader is used")

    def _upload_file(self, local_file_path: Path):
        self._client.upload_file(local_file_path, self.base_uri)

    def __del__(self):
        try:
            dbx_echo("Cleaning up the temp directory")
            self._client.remove_dir(self.base_uri)
            dbx_echo(":white_check_mark: Cleaning up the temp directory")
        except Exception as e:
            dbx_echo(f"Cannot cleanup temp directory due to {e}")
