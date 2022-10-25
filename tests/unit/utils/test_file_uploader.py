import pytest

from dbx.utils.file_uploader import MlflowFileUploader

TEST_ARTIFACT_PATHS = ["s3://some/prefix", "dbfs:/some/prefix", "adls://some/prefix", "gs://some/prefix"]


@pytest.mark.parametrize("artifact_uri", TEST_ARTIFACT_PATHS)
def test_fuse_support(artifact_uri, mocker):
    mocker.patch("mlflow.log_artifact", return_value=None)
    mocker.patch("dbx.utils.file_uploader.MlflowFileUploader._verify_reference", return_value=None)
    test_reference = "file:fuse://some-path"
    for artifact_uri in TEST_ARTIFACT_PATHS:
        uploader = MlflowFileUploader(base_uri=artifact_uri)

        if not artifact_uri.startswith("dbfs:/"):
            with pytest.raises(Exception):
                uploader.upload_and_provide_path(test_reference)
        else:
            resulting_path = uploader.upload_and_provide_path(test_reference)
            expected_path = "/".join(
                [artifact_uri.replace("dbfs:/", "/dbfs/"), test_reference.replace("file:fuse://", "")]
            )
            assert expected_path == resulting_path
