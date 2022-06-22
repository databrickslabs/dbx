import shutil
from pathlib import Path
from unittest.mock import Mock

import mlflow
import pytest
import yaml
from databricks_cli.sdk import JobsService, ApiClient
from requests import HTTPError

from dbx.api.config_reader import ConfigReader
from dbx.api.configure import EnvironmentInfo, ConfigurationManager
from dbx.api.storage.mlflow_based import MlflowStorageConfigurationManager
from dbx.commands.deploy import deploy, _update_job, _log_dbx_file, _create_job, _preprocess_jobs  # noqa
from dbx.utils.json import JsonUtils
from .conftest import invoke_cli_runner, get_path_with_relation_to_current_file


def test_deploy_smoke_default(temp_project: Path, mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client):
    deploy_result = invoke_cli_runner(deploy)
    assert deploy_result.exit_code == 0


def test_deploy_files_only_smoke_default(
    temp_project: Path, mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client
):
    deploy_result = invoke_cli_runner(deploy, ["--files-only"])
    assert deploy_result.exit_code == 0


def test_deploy_multitask_smoke(mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client):
    samples_path = get_path_with_relation_to_current_file("../deployment-configs/")
    for file_name in ["03-multitask-job.json", "03-multitask-job.yaml"]:
        deployment_file = Path("./conf/") / file_name
        shutil.copy(samples_path / file_name, str(deployment_file))
        shutil.copy(samples_path / "placeholder_1.py", Path("./placeholder_1.py"))
        shutil.copy(samples_path / "placeholder_2.py", Path("./placeholder_2.py"))

        deploy_result = invoke_cli_runner(
            deploy,
            [
                "--environment",
                "default",
                "--deployment-file",
                str(deployment_file),
                "--write-specs-to-file",
                ".dbx/deployment-result.json",
            ],
        )
        assert deploy_result.exit_code == 0
        _content = JsonUtils.read(Path(".dbx/deployment-result.json"))
        assert "libraries" not in _content["default"]["jobs"][0]
        assert "libraries" in _content["default"]["jobs"][0]["tasks"][0]


def test_deploy_path_adjustment_json(mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client):
    samples_path = get_path_with_relation_to_current_file("../deployment-configs/")
    for file_name in ["04-path-adjustment-policy.json", "04-path-adjustment-policy.yaml"]:
        deployment_file = Path("./conf/") / file_name
        shutil.copy(samples_path / file_name, str(deployment_file))
        shutil.copy(samples_path / "placeholder_1.py", Path("./placeholder_1.py"))
        shutil.copy(samples_path / "placeholder_2.py", Path("./placeholder_2.py"))

        deploy_result = invoke_cli_runner(
            deploy,
            [
                "--environment",
                "default",
                "--deployment-file",
                str(deployment_file),
                "--write-specs-to-file",
                ".dbx/deployment-result.json",
            ],
        )
        _content = JsonUtils.read(Path(".dbx/deployment-result.json"))
        expected_prefix = mlflow.get_tracking_uri()
        assert _content["default"]["jobs"][0]["libraries"][0]["whl"].startswith(expected_prefix)
        assert _content["default"]["jobs"][0]["spark_python_task"]["python_file"].startswith(expected_prefix)
        assert _content["default"]["jobs"][0]["spark_python_task"]["parameters"][0].startswith(expected_prefix)

        assert deploy_result.exit_code == 0


def test_incorrect_location(tmp_path):
    _info = EnvironmentInfo("test", artifact_location=tmp_path.as_uri())
    MlflowStorageConfigurationManager._setup_experiment(_info)
    _wrong_info = EnvironmentInfo("test", workspace_dir=_info.workspace_dir, artifact_location=tmp_path.parent.as_uri())
    with pytest.raises(Exception):
        MlflowStorageConfigurationManager._setup_experiment(_wrong_info)


def test_non_existent_env(mock_api_v2_client):
    env_name = "configured-but-not-provided"
    ConfigurationManager().create_or_update(env_name, EnvironmentInfo("test"))
    deploy_result = invoke_cli_runner(deploy, ["--environment", env_name], expected_error=True)
    assert isinstance(deploy_result.exception, NameError)
    assert "non-existent in the deployment file" in str(deploy_result.exception)


def test_deploy_only_chosen_jobs(mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client):
    result_file = ".dbx/deployment-result.json"
    _chosen = [j["name"] for j in ConfigReader().get_environment("default")["jobs"]][:2]
    deploy_result = invoke_cli_runner(
        deploy,
        ["--environment", "default", "--jobs", ",".join(_chosen), "--write-specs-to-file", result_file],
    )
    assert deploy_result.exit_code == 0
    _content = JsonUtils.read(Path(result_file))
    assert _chosen == [j["name"] for j in _content["default"]["jobs"]]


def test_negative_both_arguments(mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client):
    result_file = ".dbx/deployment-result.json"
    _chosen = [j["name"] for j in ConfigReader().get_environment("default")["jobs"]][:2]
    deploy_result = invoke_cli_runner(
        deploy,
        [
            "--environment",
            "default",
            "--job",
            _chosen[0],
            "--jobs",
            ",".join(_chosen),
            "--write-specs-to-file",
            result_file,
        ],
        expected_error=True,
    )
    assert "cannot be provided together" in str(deploy_result.exception)


def test_deploy_with_requirements_and_branch(mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client):
    sample_requirements = "\n".join(["pyspark==3.0.0", "xgboost==0.6.0", "pyspark3d"])
    Path("runtime_requirements.txt").write_text(sample_requirements)

    deploy_result = invoke_cli_runner(
        deploy,
        [
            "--requirements-file",
            "runtime_requirements.txt",
            "--branch-name",
            "test-branch",
        ],
    )

    deleted_dependency_lines = [
        line for line in deploy_result.stdout.splitlines() if "pyspark dependency deleted" in line
    ]
    assert len(deleted_dependency_lines) == 1

    assert deploy_result.exit_code == 0


def test_smoke_update_job_positive():
    js = Mock(JobsService)
    _update_job(js, "aa-bbb-ccc-111", {"name": 1})


def test_smoke_update_job_negative():
    js = Mock(JobsService)
    js.reset_job.side_effect = Mock(side_effect=HTTPError())
    with pytest.raises(HTTPError):
        _update_job(js, "aa-bbb-ccc-111", {"name": 1})


def test_create_job_with_error():
    client = Mock(ApiClient)
    client.perform_query.side_effect = Mock(side_effect=HTTPError())
    with pytest.raises(HTTPError):
        _create_job(client, {"name": "some-job"})


def test_preprocess_jobs():
    with pytest.raises(Exception):
        _preprocess_jobs([], ["some-job-name"])


def test_with_permissions(mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client):
    deployment_file = Path("conf/deployment.yml")
    deploy_content = yaml.safe_load(deployment_file.read_text())

    sample_job = deploy_content.get("environments").get("default").get("jobs")[0]

    sample_job["permissions"] = {
        "access_control_list": [
            {
                "user_name": "some_user@example.com",
                "permission_level": "IS_OWNER",
            },
            {"group_name": "some-user-group", "permission_level": "CAN_VIEW"},
        ]
    }

    deployment_file.write_text(yaml.safe_dump(deploy_content))

    deploy_result = invoke_cli_runner(deploy)

    assert deploy_result.exit_code == 0


def test_jinja_custom_path(mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client, temp_project: Path):
    samples_path = get_path_with_relation_to_current_file("../deployment-configs/")
    nested_config_dir = samples_path / "nested-configs"
    shutil.copytree(nested_config_dir, temp_project.parent / "configs")
    (temp_project / "conf" / "deployment.yml").unlink()
    shutil.copy(samples_path / "placeholder_1.py", Path("./placeholder_1.py"))
    deploy_result = invoke_cli_runner(deploy, ["--deployment-file", "../configs/09-jinja-include.json.j2"])
    assert deploy_result.exit_code == 0
