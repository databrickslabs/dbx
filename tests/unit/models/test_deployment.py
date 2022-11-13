import pytest
import yaml

from dbx.api.config_reader import ConfigReader
from dbx.models.deployment import DeploymentConfig, EnvironmentDeploymentInfo, WorkflowListMixin, Deployment
from dbx.models.workflow.common.pipeline import Pipeline
from dbx.models.workflow.v2dot0.workflow import Workflow as V2dot0Workflow
from dbx.models.workflow.v2dot1.workflow import Workflow as V2dot1Workflow
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


def test_raise_if_not_found():
    _env1 = EnvironmentDeploymentInfo(name="n1", payload={})
    _dc = DeploymentConfig(environments=[_env1, _env1])
    with pytest.raises(Exception):
        _dc.get_environment("n3", raise_if_not_found=True)

    assert _dc.get_environment("n3") is None


def test_build_payload(capsys):
    _payload = DeploymentConfig._prepare_build({"build": {"commands": ["sleep 5"]}})
    res = capsys.readouterr()
    assert "No build logic defined in the deployment file" not in res.out
    assert _payload.commands is not None


def test_build_payload_warning(capsys):
    _payload = DeploymentConfig._prepare_build({})
    res = capsys.readouterr()
    assert "No build logic defined in the deployment file" in res.out


def test_legacy_build_conflict():
    with pytest.raises(ValueError) as exc_info:
        DeploymentConfig.from_legacy_json_payload({"build": {"some": "value"}})
    assert "Deployment file with a legacy syntax" in str(exc_info)


def test_empty_spec():
    with pytest.raises(ValueError):
        EnvironmentDeploymentInfo.from_spec("test", {})


def test_workflows_list_duplicates():
    with pytest.raises(ValueError):
        WorkflowListMixin(
            **{"workflows": [{"name": "a", "workflow_type": "job-v2.1"}, {"name": "a", "workflow_type": "job-v2.1"}]}
        )


def test_workflows_list_bad_get():
    _wf = WorkflowListMixin(**{"workflows": [{"name": "a", "workflow_type": "job-v2.1"}]})
    with pytest.raises(ValueError):
        _wf.get_workflow("b")


def test_various_workflow_definitions():
    test_payload = """
    workflows:
    - name: "dlt-pipeline"
      workflow_type: "pipeline"
    - name: "job-v21"
      tasks:
        - task_key: "first"
          spark_python_task:
            python_file: "/some/file.py"
    - name: "job-v20"
      spark_python_task:
        python_file: "/some/file.py"
    """
    _dep = Deployment.from_spec_remote(yaml.safe_load(test_payload))
    assert isinstance(_dep.get_workflow("dlt-pipeline"), Pipeline)
    assert isinstance(_dep.get_workflow("job-v21"), V2dot1Workflow)
    assert isinstance(_dep.get_workflow("job-v20"), V2dot0Workflow)
