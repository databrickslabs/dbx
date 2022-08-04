from pathlib import PurePosixPath, PureWindowsPath, Path
from unittest.mock import patch, MagicMock

import pytest

from dbx.utils.file_uploader import MlflowFileUploader, ContextBasedUploader

TEST_ARTIFACT_PATHS = ["s3://some/prefix", "dbfs:/some/prefix", "adls://some/prefix", "gs://some/prefix"]


@patch("mlflow.log_artifact", return_value=None)
def test_mlflow_uploader(_):
    local_paths = [PurePosixPath("/some/local/file"), PureWindowsPath("C:\\some\\file")]

    for artifact_uri in TEST_ARTIFACT_PATHS:
        for local_path in local_paths:
            uploader = MlflowFileUploader(base_uri=artifact_uri)
            resulting_path = uploader.upload_and_provide_path(local_path)
            expected_path = "/".join([artifact_uri, str(local_path.as_posix())])
            assert expected_path == resulting_path


def test_context_uploader():
    local_paths = [PurePosixPath("/some/local/file"), PureWindowsPath("C:\\some\\file")]
    client = MagicMock()
    base_uri = "/tmp/some/path"
    client.get_temp_dir = MagicMock(return_value=base_uri)

    for local_path in local_paths:
        uploader = ContextBasedUploader(client)
        resulting_path = uploader.upload_and_provide_path(local_path)
        expected_path = "/".join([base_uri, str(local_path.as_posix())])
        assert expected_path == resulting_path


@patch("mlflow.log_artifact", return_value=None)
def test_fuse_support(_):
    local_path = Path("/some/local/file")
    for artifact_uri in TEST_ARTIFACT_PATHS:
        uploader = MlflowFileUploader(base_uri=artifact_uri)

        if not artifact_uri.startswith("dbfs:/"):
            with pytest.raises(Exception):
                uploader.upload_and_provide_path(local_path, as_fuse=True)
        else:
            resulting_path = uploader.upload_and_provide_path(local_path, as_fuse=True)
            expected_path = "/".join([artifact_uri.replace("dbfs:/", "/dbfs/"), str(local_path.as_posix())])
            assert expected_path == resulting_path
