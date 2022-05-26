import pathlib
import unittest
from unittest.mock import patch

from dbx.utils.file_uploader import FileUploader


class FileUploaderTest(unittest.TestCase):
    TEST_ARTIFACT_PATHS = ["s3://some/prefix", "dbfs:/some/prefix", "adl://some/prefix", "gs://some/prefix"]

    @patch("mlflow.log_artifact", return_value=None)
    def test_uploader(self, _):

        local_paths = [pathlib.PurePosixPath("/some/local/file"), pathlib.PureWindowsPath("C:\\some\\file")]

        for artifact_uri in self.TEST_ARTIFACT_PATHS:
            for local_path in local_paths:
                uploader = FileUploader(artifact_uri=artifact_uri)
                resulting_path = uploader.upload_and_provide_path(local_path)
                expected_path = "/".join([artifact_uri, str(local_path.as_posix())])
                self.assertEqual(expected_path, resulting_path)

    @patch("mlflow.log_artifact", return_value=None)
    def test_fuse_support(self, _):

        local_path = pathlib.Path("/some/local/file")

        for artifact_uri in self.TEST_ARTIFACT_PATHS:
            uploader = FileUploader(artifact_uri=artifact_uri)

            if not artifact_uri.startswith("dbfs:/"):
                with self.assertRaises(Exception):
                    _ = uploader.upload_and_provide_path(local_path, as_fuse=True)
            else:
                resulting_path = uploader.upload_and_provide_path(local_path, as_fuse=True)
                expected_path = "/".join([artifact_uri.replace("dbfs:/", "/dbfs/"), str(local_path.as_posix())])
                self.assertEqual(expected_path, resulting_path)
