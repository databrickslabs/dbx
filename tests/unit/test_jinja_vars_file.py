import json
import shutil
from pathlib import Path

from dbx.api.config_reader import ConfigReader
from dbx.commands.deploy import deploy
from .conftest import invoke_cli_runner, get_path_with_relation_to_current_file


def test_incorrect_file_name(temp_project: Path, mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client):
    deploy_result = invoke_cli_runner(deploy, ["--jinja-variables-file", "some-file.py"], expected_error=True)
    assert "Jinja variables file shall be provided" in str(deploy_result.exception)


def test_non_existent_file(temp_project: Path, mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client):
    deploy_result = invoke_cli_runner(deploy, ["--jinja-variables-file", "some-non-existent.yml"], expected_error=True)
    assert "file is non-existent" in str(deploy_result.exception)


def test_jinja_vars_file(mlflow_file_uploader, mock_dbx_file_upload, mock_api_v2_client, temp_project: Path):
    jinja_vars_dir = get_path_with_relation_to_current_file("../deployment-configs/jinja-vars/")
    project_config_dir = temp_project / "conf"
    vars_file = Path("./conf/jinja-template-variables-file.yaml")
    shutil.rmtree(project_config_dir)
    shutil.copytree(jinja_vars_dir, project_config_dir)
    definitions = []
    for _dep_file in project_config_dir.glob("*.j2"):
        env = ConfigReader(_dep_file, vars_file).get_environment("default")
        definitions.append(json.dumps(env, indent=4))

    # check if all definitions are same
    for _def in definitions:
        assert _def == definitions[0]
