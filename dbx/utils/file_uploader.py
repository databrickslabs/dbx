import pathlib
from typing import Optional, Dict

import mlflow
from retry import retry

from dbx.utils import dbx_echo


class MlflowFileUploader:
    """
    MlflowFileUploader represents a class that is used for uploading local files into mlflow storage
    """

    def __init__(self, artifact_uri: str):
        """
        artifact_uri - base location of files for mlflow
        """
        self._artifact_uri = artifact_uri
        self._uploaded_files: Dict[
            pathlib.Path, str
        ] = {}  # contains mapping from local to remote paths for all uploaded files

    @staticmethod
    @retry(tries=3, delay=1, backoff=0.3)
    def _upload_file(file_path: pathlib.Path):
        posix_path_str = file_path.as_posix()
        posix_path = pathlib.PurePosixPath(posix_path_str)
        dbx_echo(f"Uploading file: {file_path}")
        mlflow.log_artifact(str(file_path), str(posix_path.parent))

    def _verify_fuse_support(self):
        if not self._artifact_uri.startswith("dbfs:/"):
            raise Exception(
                "Fuse-based paths are not supported for non-dbfs artifact locations."
                "If fuse-like paths are required, consider using DBFS mount as artifact location."
            )

    def upload_and_provide_path(self, local_path: pathlib.Path, as_fuse: Optional[bool] = False) -> str:
        if as_fuse:
            self._verify_fuse_support()

        if local_path in self._uploaded_files:
            dbx_echo("File is already uploaded, returning it's path to the definition")
            remote_path = self._uploaded_files[local_path]
        else:
            self._upload_file(local_path)
            remote_path = "/".join([self._artifact_uri, str(local_path.as_posix())])
            self._uploaded_files[local_path] = remote_path

        remote_path = remote_path.replace("dbfs:/", "/dbfs/") if as_fuse else remote_path
        return remote_path
