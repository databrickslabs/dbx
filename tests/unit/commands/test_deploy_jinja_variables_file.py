import shutil
from pathlib import Path

from dbx.api.config_reader import ConfigReader, Jinja2ConfigReader
from dbx.api.configure import ProjectConfigurationManager
from tests.unit.conftest import (
    get_path_with_relation_to_current_file,
    invoke_cli_runner,
)


def test_incorrect_file_name(temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client):
    deploy_result = invoke_cli_runner(["deploy", "--jinja-variables-file", "some-file.py"], expected_error=True)
    assert "Jinja variables file shall be provided" in str(deploy_result.exception)


def test_non_existent_file(temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client):
    deploy_result = invoke_cli_runner(
        ["deploy", "--jinja-variables-file", "some-non-existent.yml"], expected_error=True
    )
    assert "file is non-existent" in str(deploy_result.exception)


def test_passed_with_unsupported(temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client):
    file_name = "jinja-template-variables-file.yaml"
    src_vars_file = get_path_with_relation_to_current_file(f"../deployment-configs/jinja-vars/{file_name}")
    dst_vars_file = Path("./conf") / file_name
    shutil.copy(src_vars_file, dst_vars_file)
    ProjectConfigurationManager().disable_jinja_support()

    deploy_result = invoke_cli_runner(["deploy", "--jinja-variables-file", str(dst_vars_file)], expected_error=True)

    assert "deployment file is not based on Jinja" in str(deploy_result.exception)


def test_passed_with_inplace(temp_project: Path, mlflow_file_uploader, mock_storage_io, mock_api_v2_client):
    invoke_cli_runner(["configure", "--enable-inplace-jinja-support"])
    file_name = "jinja-template-variables-file.yaml"
    src_vars_file = get_path_with_relation_to_current_file(f"../deployment-configs/jinja-vars/{file_name}")
    dst_vars_file = Path("./conf") / file_name
    shutil.copy(src_vars_file, dst_vars_file)
    rdr = ConfigReader(Path("conf/deployment.yml"), dst_vars_file)._define_reader()
    assert isinstance(rdr, Jinja2ConfigReader)


def test_jinja_vars_file_api(mlflow_file_uploader, mock_storage_io, mock_api_v2_client, temp_project: Path):
    jinja_vars_dir = get_path_with_relation_to_current_file("../deployment-configs/jinja-vars/")
    project_config_dir = temp_project / "conf"
    vars_file = Path("./conf/jinja-template-variables-file.yaml")
    shutil.rmtree(project_config_dir)
    shutil.copytree(jinja_vars_dir, project_config_dir)
    definitions = []
    for _dep_file in project_config_dir.glob("*.j2"):
        definition = ConfigReader(_dep_file, vars_file).get_environment("default")
        definitions.append(definition)

    # check if all definitions are same
    for _def in definitions:
        assert _def == definitions[0]


def test_jinja_vars_file_cli(mlflow_file_uploader, mock_storage_io, mock_api_v2_client, temp_project: Path):
    deployment_file_name = "09-jinja-with-custom-vars.yaml.j2"
    vars_file_name = "jinja-template-variables-file.yaml"
    project_config_dir = temp_project / "conf"

    src_vars_file = get_path_with_relation_to_current_file(f"../deployment-configs/jinja-vars/{vars_file_name}")
    dst_vars_file = project_config_dir / vars_file_name

    src_deployment_file = get_path_with_relation_to_current_file(
        f"../deployment-configs/jinja-vars/{deployment_file_name}"
    )
    dst_deployment_file = project_config_dir / deployment_file_name

    shutil.rmtree(project_config_dir)
    project_config_dir.mkdir()
    shutil.copy(src_vars_file, dst_vars_file)
    shutil.copy(src_deployment_file, dst_deployment_file)

    deploy_result = invoke_cli_runner(
        ["deploy", f"--deployment-file", str(dst_deployment_file), "--jinja-variables-file", str(dst_vars_file)]
    )

    assert deploy_result.exit_code == 0
