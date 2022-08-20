import pytest

from dbx.api.config_reader import ConfigReader
from dbx.models.deployment import DeploymentConfig, EnvironmentDeploymentInfo
from tests.unit.conftest import get_path_with_relation_to_current_file


def test_jobs_deprecated_message(capsys, temp_project):
    configs_path = get_path_with_relation_to_current_file("../deployment-configs/")
    _env = ConfigReader(configs_path / "01-yaml-test.yaml").get_environment("default")
    captured = capsys.readouterr()
    assert len(_env.payload.workflows) == 1
    assert "Usage of jobs keyword in deployment file" in captured.out


def test_workflows_consistency(capsys, temp_project):
    configs_path = get_path_with_relation_to_current_file("../deployment-configs/")
    _old = ConfigReader(configs_path / "10-multitask-job-jobs.yaml").get_environment("default")
    _new = ConfigReader(configs_path / "10-multitask-job-workflows.yaml").get_environment("default")

    assert _old == _new


def test_non_unique_environments():
    _env1 = EnvironmentDeploymentInfo(name="n1", payload={})
    _dc = DeploymentConfig(environments=[_env1, _env1])
    with pytest.raises(Exception):
        _dc.get_environment("n1")


def test_build_payload(capsys):
    _payload = DeploymentConfig.prepare_build({"build": {"commands": ["sleep 5"]}})
    res = capsys.readouterr()
    assert "No build logic defined in the deployment file" not in res.out
    assert _payload.commands is not None


def test_build_payload_warning(capsys):
    _payload = DeploymentConfig.prepare_build({})
    res = capsys.readouterr()
    assert "No build logic defined in the deployment file" in res.out


def test_legacy_build_conflict():
    with pytest.raises(ValueError) as exc_info:
        DeploymentConfig.from_legacy_json_payload({"build": {"some": "value"}})
    assert "Deployment file with a legacy syntax" in str(exc_info)
