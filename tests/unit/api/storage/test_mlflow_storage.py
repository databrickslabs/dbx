import mlflow

from dbx.api.storage.mlflow_based import MlflowStorageConfigurationManager
from dbx.models.project import EnvironmentInfo, MlflowStorageProperties


def test_url_strip():
    _stripped = MlflowStorageConfigurationManager._strip_url("https://some-location.com/with/some-postfix")
    assert _stripped == "https://some-location.com"


def test_experiment_setup(tmp_path):
    _info = EnvironmentInfo(
        profile="test",
        properties=MlflowStorageProperties(
            artifact_location=tmp_path.as_uri(), workspace_directory=f"/Shared/dbx/{tmp_path.name}"
        ),
    )
    MlflowStorageConfigurationManager._setup_experiment(_info)
    assert mlflow.get_experiment_by_name(f"/Shared/dbx/{tmp_path.name}") is not None
