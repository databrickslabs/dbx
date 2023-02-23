import shutil
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from pytest_mock import MockerFixture

from dbx.api.config_reader import ConfigReader
from dbx.api.configure import EnvironmentInfo, ProjectConfigurationManager
from dbx.api.services.jobs import NamedJobsService
from dbx.api.storage.mlflow_based import MlflowStorageConfigurationManager
from dbx.commands import deploy
from dbx.models.files.project import MlflowStorageProperties
from dbx.utils.json import JsonUtils
from tests.unit.conftest import (
    get_path_with_relation_to_current_file,
    invoke_cli_runner,
)


def test_deploy_smoke_default(temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client):
    deploy_result = invoke_cli_runner("deploy")
    assert deploy_result.exit_code == 0


@pytest.mark.parametrize("argset", [["--files-only"], ["--assets-only"], ["--no-rebuild"]])
def test_deploy_assets_only_smoke_default(
    argset, temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client
):
    deploy_result = invoke_cli_runner(["deploy"] + argset)
    assert deploy_result.exit_code == 0


def test_deploy_assets_pipeline(temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client):
    (temp_project / "conf" / "deployment.yml").write_text(
        """
    environments:
      default:
        workflows:
          - name: "pipe"
            workflow_type: "pipeline"
            libraries:
              - notebook:
                  path: "/some/path"
    """
    )
    deploy_result = invoke_cli_runner(["deploy", "--assets-only"], expected_error=True)
    assert "not supported for DLT pipelines" in str(deploy_result.exception)


def test_deploy_multitask_smoke(
    mlflow_file_uploader, mocker: MockerFixture, mock_storage_io, mock_api_v2_client, temp_project
):
    mocker.patch.object(NamedJobsService, "create", MagicMock(return_value=1))
    samples_path = get_path_with_relation_to_current_file("../deployment-configs/")
    for file_name in ["03-multitask-job.json", "03-multitask-job.yaml"]:
        deployment_file = Path("./conf/") / file_name
        shutil.copy(samples_path / file_name, str(deployment_file))
        shutil.copy(samples_path / "placeholder_1.py", Path("./placeholder_1.py"))
        shutil.copy(samples_path / "placeholder_2.py", Path("./placeholder_2.py"))

        deploy_result = invoke_cli_runner(
            [
                "deploy",
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
        assert "libraries" not in _content["default"]["workflows"][0]
        assert "libraries" in _content["default"]["workflows"][0]["tasks"][0]


def test_deploy_path_adjustment_json(mlflow_file_uploader, mocker, mock_storage_io, mock_api_v2_client, temp_project):
    mocker.patch.object(NamedJobsService, "create", MagicMock(return_value=1))
    samples_path = get_path_with_relation_to_current_file("../deployment-configs/")
    for file_name in ["04-path-adjustment-policy.json", "04-path-adjustment-policy.yaml"]:
        deployment_file = Path("./conf/") / file_name
        shutil.copy(samples_path / file_name, str(deployment_file))
        shutil.copy(samples_path / "placeholder_1.py", Path("./placeholder_1.py"))
        shutil.copy(samples_path / "placeholder_2.py", Path("./placeholder_2.py"))

        deploy_result = invoke_cli_runner(
            [
                "deploy",
                "--environment",
                "default",
                "--deployment-file",
                str(deployment_file),
                "--write-specs-to-file",
                ".dbx/deployment-result.json",
                "--debug",
            ],
        )
        _content = JsonUtils.read(Path(".dbx/deployment-result.json"))
        expected_prefix = "dbfs:/mocks/testing"

        assert _content["default"]["workflows"][0]["libraries"][0]["whl"].startswith(expected_prefix)
        assert _content["default"]["workflows"][0]["spark_python_task"]["python_file"].startswith(expected_prefix)
        assert _content["default"]["workflows"][0]["spark_python_task"]["parameters"][0].startswith("/dbfs/")

        assert deploy_result.exit_code == 0


def test_incorrect_location(tmp_path):
    _info = EnvironmentInfo(
        profile="test",
        properties=MlflowStorageProperties(
            artifact_location=tmp_path.as_uri(), workspace_directory=f"/Shared/dbx/{tmp_path.name}"
        ),
    )
    MlflowStorageConfigurationManager._setup_experiment(_info)
    _wrong_info = EnvironmentInfo(
        profile="test" "test",
        properties=MlflowStorageProperties(
            workspace_directory=_info.properties.workspace_directory, artifact_location=tmp_path.parent.as_uri()
        ),
    )
    with pytest.raises(Exception):
        MlflowStorageConfigurationManager._setup_experiment(_wrong_info)


def test_non_existent_env(mock_api_v2_client, temp_project):
    env_name = "configured-but-not-provided"
    ProjectConfigurationManager().create_or_update(
        env_name,
        EnvironmentInfo(
            profile="test",
            properties=MlflowStorageProperties(
                workspace_directory="/Shared/dbx/test", artifact_location="dbfs:/dbx/test"
            ),
        ),
    )
    deploy_result = invoke_cli_runner(["deploy", "--environment", env_name], expected_error=True)
    assert isinstance(deploy_result.exception, NameError)
    assert "not found in the deployment file" in str(deploy_result.exception)


def test_deploy_only_chosen_workflow(mlflow_file_uploader, mocker, mock_storage_io, mock_api_v2_client, temp_project):
    mocker.patch.object(NamedJobsService, "create", MagicMock(return_value=1))
    result_file = ".dbx/deployment-result.json"
    deployment_info = ConfigReader(Path("conf/deployment.yml")).get_environment("default")
    _chosen = deployment_info.payload.workflow_names[0]
    deploy_result = invoke_cli_runner(
        ["deploy", "--environment=default", f"--write-specs-to-file={result_file}", _chosen],
    )
    assert deploy_result.exit_code == 0
    _content = JsonUtils.read(Path(result_file))
    assert [w["name"] for w in _content["default"]["workflows"]] == [_chosen]


@pytest.mark.parametrize("workflow_arg", ["--workflows", "--jobs"])
def test_deploy_only_chosen(
    workflow_arg, mlflow_file_uploader, mocker, mock_storage_io, mock_api_v2_client, temp_project
):
    mocker.patch.object(NamedJobsService, "create", MagicMock(return_value=1))
    result_file = ".dbx/deployment-result.json"
    deployment_info = ConfigReader(Path("conf/deployment.yml")).get_environment("default")
    _chosen = deployment_info.payload.workflow_names[:2]
    deploy_result = invoke_cli_runner(
        ["deploy", "--environment", "default", workflow_arg, ",".join(_chosen), "--write-specs-to-file", result_file],
    )
    assert deploy_result.exit_code == 0
    _content = JsonUtils.read(Path(result_file))
    assert [w["name"] for w in _content["default"]["workflows"]] == _chosen


def test_negative_both_arguments(mlflow_file_uploader, mock_storage_io, mock_api_v2_client, temp_project):
    result_file = ".dbx/deployment-result.json"
    deployment_info = ConfigReader(Path("conf/deployment.yml")).get_environment("default")
    _chosen = deployment_info.payload.workflow_names[0]
    deploy_result = invoke_cli_runner(
        [
            "deploy",
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


def test_deploy_with_requirements_and_branch(mlflow_file_uploader, mock_storage_io, mock_api_v2_client, temp_project):
    sample_requirements = "\n".join(["pyspark==3.0.0", "xgboost==0.6.0", "pyspark3d"])
    Path("runtime_requirements.txt").write_text(sample_requirements)

    deploy_result = invoke_cli_runner(
        [
            "deploy",
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


def test_with_permissions(mocker, mlflow_file_uploader, mock_storage_io, mock_api_v2_client, temp_project):
    mocker.patch.object(NamedJobsService, "create", MagicMock(return_value=1))
    deployment_file = Path("conf/deployment.yml")
    deploy_content = yaml.safe_load(deployment_file.read_text())

    sample_job = deploy_content.get("environments").get("default").get("workflows")[0]

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

    deploy_result = invoke_cli_runner("deploy")

    assert deploy_result.exit_code == 0


def test_jinja_custom_path(mlflow_file_uploader, mock_storage_io, mock_api_v2_client, temp_project: Path):
    samples_path = get_path_with_relation_to_current_file("../deployment-configs/")
    nested_config_dir = samples_path / "nested-configs"
    shutil.copytree(nested_config_dir, temp_project.parent / "configs")
    (temp_project / "conf" / "deployment.yml").unlink()
    shutil.copy(samples_path / "placeholder_1.py", Path("./placeholder_1.py"))
    deploy_result = invoke_cli_runner(["deploy", "--deployment-file", "../configs/09-jinja-include.json.j2"])
    assert deploy_result.exit_code == 0


def test_deploy_empty_workflows_list(temp_project, mlflow_file_uploader, mock_storage_io, mock_api_v2_client):
    payload = textwrap.dedent(
        """\
    environments:
      default:
        workflows: []
    """
    )
    Path("conf/deployment.yml").write_text(payload)
    deploy_result = invoke_cli_runner("deploy")
    assert deploy_result.exit_code == 0


@patch(f"{deploy.__name__}.CorePackageManager")
@patch(f"{deploy.__name__}.BuildProperties")
def test_deploy_with_no_package(
    mock_build_properties,
    mock_core_package_manager,
    mlflow_file_uploader,
    mocker,
    mock_storage_io,
    mock_api_v2_client,
    temp_project,
):
    mocker.patch.object(NamedJobsService, "create", MagicMock(return_value=1))
    result_file = ".dbx/deployment-result.json"
    _ = ConfigReader(Path("conf/deployment.yml")).get_environment("default")
    deploy_result = invoke_cli_runner(
        ["deploy", "--environment", "default", "--no-package", "--write-specs-to-file", result_file],
    )
    assert deploy_result.exit_code == 0
    mock_build_properties.assert_called_once_with(potential_build=True, no_rebuild=True)
    mock_core_package_manager.assert_not_called()
    _content = JsonUtils.read(Path(result_file))
    assert not any([t["libraries"] for w in _content["default"]["workflows"] for t in w["tasks"]])


@pytest.mark.usefixtures("temp_project", "mlflow_file_uploader", "mock_storage_io", "mock_api_v2_client")
def test_deploy_additional_headers(mocker: MockerFixture):
    expected_headers = {
        "azure_sp_token": "eyJhbAAAABBBB",
        "workspace_id": (
            "/subscriptions/bc5bAAA-BBBB/resourceGroups/some-resource-group"
            "/providers/Microsoft.Databricks/workspaces/target-dtb-ws"
        ),
        "org_id": "1928374655647382",
    }
    env_mock = mocker.patch("dbx.commands.deploy.prepare_environment", MagicMock())
    header_parse_mock = mocker.patch("dbx.commands.deploy.parse_multiple", wraps=deploy.parse_multiple)
    kwargs = [f"{key}={val}" for key, val in expected_headers.items()]
    cli_kwargs = " ".join([f"--header {kw}" for kw in kwargs])
    deploy_result = invoke_cli_runner(f"deploy {cli_kwargs}")
    assert deploy_result.exit_code == 0
    header_parse_mock.assert_any_call(kwargs)
    env_mock.assert_called_once_with("default", expected_headers)
