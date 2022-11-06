from unittest.mock import MagicMock

import pytest
from mlflow.exceptions import RestException
from mlflow.protos.databricks_pb2 import ErrorCode, INVALID_PARAMETER_VALUE, INTERNAL_ERROR
from pytest_mock import MockerFixture

from dbx.api.storage.mlflow_based import MlflowStorageConfigurationManager
from dbx.models.files.project import EnvironmentInfo, MlflowStorageProperties
from dbx.utils.url import strip_databricks_url


def test_url_strip():
    _stripped = strip_databricks_url("https://some-location.com/with/some-postfix")
    assert _stripped == "https://some-location.com"


def test_experiment_setup(tmp_path, mocker: MockerFixture):
    _info = EnvironmentInfo(
        profile="test",
        properties=MlflowStorageProperties(
            artifact_location=tmp_path.as_uri(), workspace_directory=f"/Shared/dbx/{tmp_path.name}"
        ),
    )

    setup_mocker = MagicMock()
    mocker.patch("mlflow.set_experiment", setup_mocker)
    MlflowStorageConfigurationManager._setup_experiment(_info)
    setup_mocker.assert_called_once()


def test_experiment_exception(tmp_path, mocker: MockerFixture):
    exception_content = RestException(
        {"error_code": ErrorCode.Name(INVALID_PARAMETER_VALUE), "message": "Experiment with id '0' does not exist."}
    )
    mocker.patch("mlflow.get_experiment_by_name", MagicMock(side_effect=exception_content))
    resp = MlflowStorageConfigurationManager._get_experiment_safe("some")
    assert resp is None

    another_exception = RestException(
        {"error_code": ErrorCode.Name(INTERNAL_ERROR), "message": "something else happened"}
    )
    mocker.patch("mlflow.get_experiment_by_name", MagicMock(side_effect=another_exception))
    with pytest.raises(RestException):
        MlflowStorageConfigurationManager._get_experiment_safe("other")
