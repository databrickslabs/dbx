from dbx.api.config_reader import ConfigReader
from tests.unit.conftest import get_path_with_relation_to_current_file


def test_jobs_deprecated_message(capsys):
    configs_path = get_path_with_relation_to_current_file("../deployment-configs/")
    _env = ConfigReader(configs_path / "01-yaml-test.yaml").get_environment("default")
    captured = capsys.readouterr()
    assert len(_env.payload.workflows) == 1
    assert "Usage of jobs keyword in deployment file" in captured.out


def test_workflows_consistency(capsys):
    configs_path = get_path_with_relation_to_current_file("../deployment-configs/")
    _old = ConfigReader(configs_path / "10-multitask-job-jobs.yaml").get_environment("default")
    _new = ConfigReader(configs_path / "10-multitask-job-workflows.yaml").get_environment("default")

    assert _old == _new
