from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from dbx.api.storage.mlflow_based import MlflowStorageConfigurationManager
from dbx.models.project import EnvironmentInfo, MlflowStorageProperties


def test_url_strip():
    _stripped = MlflowStorageConfigurationManager._strip_url("https://some-location.com/with/some-postfix")
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
