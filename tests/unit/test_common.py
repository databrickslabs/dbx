import os
import shutil
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import git
import pytest
from databricks_cli.sdk import JobsService
from pytest_mock import MockFixture

from dbx.api.config_reader import ConfigReader
from dbx.utils.adjuster import path_adjustment, adjust_path
from dbx.utils.common import get_environment_data, generate_filter_string, handle_package, get_current_branch_name
from dbx.utils.job_listing import find_job_by_name
from .conftest import get_path_with_relation_to_current_file

json_file_01 = get_path_with_relation_to_current_file("../deployment-configs/01-json-test.json")
yaml_file_01 = get_path_with_relation_to_current_file("../deployment-configs/01-yaml-test.yaml")
jinja_json_file_01 = get_path_with_relation_to_current_file("../deployment-configs/01-jinja-test.json.j2")
jinja_yaml_file_01 = get_path_with_relation_to_current_file("../deployment-configs/01-jinja-test.yaml.j2")

json_j2_file_04 = get_path_with_relation_to_current_file("../deployment-configs/04-jinja-with-env-vars.json.j2")
yaml_j2_file_04 = get_path_with_relation_to_current_file("../deployment-configs/04-jinja-with-env-vars.yaml.j2")
json_j2_file_06 = get_path_with_relation_to_current_file("../deployment-configs/06-jinja-with-logic.json.j2")
yaml_j2_file_06 = get_path_with_relation_to_current_file("../deployment-configs/06-jinja-with-logic.yaml.j2")
json_j2_file_09 = get_path_with_relation_to_current_file(
    "../deployment-configs/nested-configs/09-jinja-include.json.j2"
)


def test_all_file_formats_can_be_read():
    json_default_envs = ConfigReader(json_file_01).get_all_environment_names()
    yaml_default_envs = ConfigReader(yaml_file_01).get_all_environment_names()
    jinja_json_default_envs = ConfigReader(jinja_json_file_01).get_all_environment_names()
    jinja_yaml_default_envs = ConfigReader(jinja_yaml_file_01).get_all_environment_names()

    assert json_default_envs == yaml_default_envs == jinja_json_default_envs == jinja_yaml_default_envs


def test_all_file_formats_contents_match():
    json_default_env = ConfigReader(json_file_01).get_environment("default")
    yaml_default_env = ConfigReader(yaml_file_01).get_environment("default")
    jinja_json_default_env = ConfigReader(jinja_json_file_01).get_environment("default")
    jinja_yaml_default_env = ConfigReader(jinja_yaml_file_01).get_environment("default")

    assert yaml_default_env == json_default_env == jinja_json_default_env == jinja_yaml_default_env


@mock.patch.dict(os.environ, {"TIMEOUT": "100"}, clear=True)
def test_jinja_files_with_env_variables_scalar_type():
    """
    JINJA2: Simple Scalar (key-value) type for timeout_seconds parameter
    """

    json_default_envs = ConfigReader(json_j2_file_04).get_environment("default")
    yaml_default_envs = ConfigReader(yaml_j2_file_04).get_environment("default")

    json_timeout_seconds = json_default_envs.get("jobs")[0].get("timeout_seconds")
    yaml_timeout_seconds = yaml_default_envs.get("jobs")[0].get("timeout_seconds")

    assert int(json_timeout_seconds) == 100
    assert int(yaml_timeout_seconds) == 100


@mock.patch.dict(os.environ, {"ALERT_EMAIL": "test@test.com"}, clear=True)
def test_jinja_files_with_env_variables_array_type():
    """
    JINJA2: In email_notification.on_failure, the first email has been set via env variables
    """
    json_default_envs = ConfigReader(json_j2_file_04).get_environment("default")
    yaml_default_envs = ConfigReader(yaml_j2_file_04).get_environment("default")

    json_emails = json_default_envs.get("jobs")[0].get("email_notifications").get("on_failure")
    yaml_emails = yaml_default_envs.get("jobs")[0].get("email_notifications").get("on_failure")

    assert json_emails == yaml_emails
    assert json_emails[0] == "test@test.com"


def test_jinja_file_with_env_variables_default_values():
    """
    JINJA:
    max_retries is set to {{ env['MAX_RETRY'] | default(3) }};
    new_cluster.aws_attributes.availability is set to {{ env['AVAILABILITY'] | default('SPOT') }}.

    MAX_RETRY and AVAILABILITY env vars will not be set. They should default to 3 and 'SPOT'
    based on config in deployment file
    """
    json_default_envs = ConfigReader(json_j2_file_04).get_environment("default")
    yaml_default_envs = ConfigReader(yaml_j2_file_04).get_environment("default")

    json_max_retries = json_default_envs.get("jobs")[0].get("max_retries")
    yaml_max_retries = yaml_default_envs.get("jobs")[0].get("max_retries")
    json_avail = json_default_envs.get("jobs")[0].get("new_cluster").get("aws_attributes").get("availability")
    yaml_avail = yaml_default_envs.get("jobs")[0].get("new_cluster").get("aws_attributes").get("availability")

    assert int(json_max_retries) == int(yaml_max_retries)
    assert int(json_max_retries) == 3
    assert json_avail == yaml_avail
    assert json_avail == "SPOT"


@mock.patch.dict(os.environ, {"ENVIRONMENT": "PRODUCTION"}, clear=True)
def test_jinja_files_with_env_variables_logic_1():
    """
    JINJA:
    - max_retries is set to {{ MAX_RETRY | default(-1) }} if (ENVIRONMENT.lower() == "production"),
      {{ MAX_RETRY | default(3) }} otherwise.
    - email_notifications are only set if (ENVIRONMENT.lower() == "production").

    ENVIRONMENT is set to "production", so MAX_RETRY and EMAIL_NOTIFICATIONS env vars
    should correspond to the ENVIRONMENT=production values.
    Also testing filters like .lower() are working correctly.
    """

    json_default_envs = ConfigReader(json_j2_file_06).get_environment("default")
    yaml_default_envs = ConfigReader(yaml_j2_file_06).get_environment("default")

    json_max_retries = json_default_envs.get("jobs")[0].get("max_retries")
    yaml_max_retries = yaml_default_envs.get("jobs")[0].get("max_retries")
    json_emails = json_default_envs.get("jobs")[0].get("email_notifications").get("on_failure")
    yaml_emails = yaml_default_envs.get("jobs")[0].get("email_notifications").get("on_failure")

    assert int(json_max_retries) == -1
    assert int(yaml_max_retries) == -1
    assert json_emails[0] == "presetEmail@test.com"
    assert yaml_emails[0] == "presetEmail@test.com"


@mock.patch.dict(os.environ, {"ENVIRONMENT": "test"}, clear=True)
def test_jinja_files_with_env_variables_logic_2():
    """
    JINJA:
    - max_retries is set to {{ MAX_RETRY | default(-1) }} if (ENVIRONMENT == "production"),
      {{ MAX_RETRY | default(3) }} otherwise.
    - email_notifications are only set if (ENVIRONMENT == "production").

    ENVIRONMENT is set to "test", so MAX_RETRY and EMAIL_NOTIFICATIONS env vars
    should correspond to the ENVIRONMENT != production values.
    """
    json_default_envs = ConfigReader(json_j2_file_06).get_environment("default")
    yaml_default_envs = ConfigReader(yaml_j2_file_06).get_environment("default")

    json_max_retries = json_default_envs.get("jobs")[0].get("max_retries")
    yaml_max_retries = yaml_default_envs.get("jobs")[0].get("max_retries")
    json_emails = json_default_envs.get("jobs")[0].get("email_notifications")
    yaml_emails = yaml_default_envs.get("jobs")[0].get("email_notifications")

    assert int(json_max_retries) == 3
    assert int(yaml_max_retries) == 3
    assert json_emails is None
    assert yaml_emails is None


def test_jinja_with_include():
    """Ensure that templates from other directories can be included.

    In this test, the top level jinja template includes another template which describes the
    cluster.
    """
    json_default_envs = ConfigReader(json_j2_file_09).get_environment("default")
    json_node_type = json_default_envs.get("jobs")[0].get("new_cluster").get("node_type_id")

    assert json_node_type == "some-node-type"


def test_get_environment_data():
    result = get_environment_data("default")
    assert result is not None
    with pytest.raises(Exception):
        _ = get_environment_data("some-non-existent-env")


def test_get_current_branch_name_gh(mocker: MockFixture):
    mocker.patch.dict("os.environ", {"GITHUB_REF": "refs/main"}, clear=True)

    assert "main" == get_current_branch_name()


def test_get_current_branch_name_no_env(mocker, temp_project: Path):
    git_path = temp_project.absolute() / ".git"
    shutil.rmtree(git_path)
    mocker.patch.dict("os.environ", {}, clear=True)
    assert get_current_branch_name() is None


def test_handle_package():
    Path("setup.py").unlink()
    with pytest.raises(FileNotFoundError):
        handle_package(None)


def test_non_existent_path_adjustment():
    with pytest.raises(FileNotFoundError):
        path_adjustment("file://some/non-existent/file", MagicMock())


def test_path_adjustment():
    dbfs_path = "dbfs:/some/path"
    _dbfs_result = adjust_path(dbfs_path, MagicMock())
    assert dbfs_path == _dbfs_result


def test_filter_string():
    output = generate_filter_string(env="test", branch_name=None)
    assert "dbx_branch_name" not in output


def test_job_listing_duplicates():
    duplicated_name = "some-name"
    jobs_payload = {
        "jobs": [
            {
                "settings": {
                    "name": duplicated_name,
                },
                "job_id": 1,
            },
            {
                "settings": {
                    "name": duplicated_name,
                },
                "job_id": 2,
            },
        ]
    }
    js = JobsService(MagicMock())
    js.list_jobs = MagicMock(return_value=jobs_payload)
    with pytest.raises(Exception):
        find_job_by_name(js, duplicated_name)
