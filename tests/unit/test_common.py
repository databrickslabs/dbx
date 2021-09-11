import unittest
from dbx.utils.common import update_json, ContextLockFile, get_deployment_config
from .utils import DbxTest
import os


def format_path(rel_path: str):
    """Format proper path for a given relative path or a glob path"""
    # required for when pytest is NOT ran from within the tests/unit dir.
    return os.path.join(os.path.dirname(__file__), rel_path)


class CommonUnitTest(unittest.TestCase):
    def test_yaml_file_can_be_read(self):
        json_file = format_path("../deployment-configs/01-yaml-test.json")
        yaml_file = format_path("../deployment-configs/01-yaml-test.yaml")
        json_default_envs = get_deployment_config(json_file).get_all_environment_names()
        yaml_default_envs = get_deployment_config(yaml_file).get_all_environment_names()
        assert json_default_envs == yaml_default_envs

    def test_yaml_file_read_will_match_the_json_file_contents(self):
        json_file = format_path("../deployment-configs/01-yaml-test.json")
        yaml_file = format_path("../deployment-configs/01-yaml-test.yaml")
        json_default_env = get_deployment_config(json_file).get_environment("default")
        yaml_default_env = get_deployment_config(yaml_file).get_environment("default")
        assert yaml_default_env == json_default_env

    def test_yaml_variables_will_result_in_equivalent_output_to_json_file(self):
        json_file = format_path("../deployment-configs/02-yaml-with-vars-test.json")
        yaml_file = format_path("../deployment-configs/02-yaml-with-vars-test.yaml")
        json_default_env = get_deployment_config(json_file).get_environment("default")
        yaml_default_env = get_deployment_config(yaml_file).get_environment("default")
        assert yaml_default_env == json_default_env

    def test_json_file_with_env_variables_scalar_type(self):
        """
        JSON: Simple Scalar (key-value) type for timeout_seconds parameter
        """
        # Set TIMEOUT env var
        os.environ["TIMEOUT"] = "100"

        json_file = format_path("../deployment-configs/03-json-with-env-vars.json")
        json_default_envs = get_deployment_config(json_file).get_environment("default")
        timeout_seconds = json_default_envs.get("jobs")[0].get("timeout_seconds")

        assert int(timeout_seconds) == 100
        del os.environ["TIMEOUT"]

    def test_json_file_with_env_variables_array_type(self):
        """
        JSON:
        In email_notification.on_failure, one email has been set via env variables
        The other email has been pre-set in deployment.yaml
        """
        # Set ALERT_EMAIL env var
        os.environ["ALERT_EMAIL"] = "test@test.com"

        json_file = format_path("../deployment-configs/03-json-with-env-vars.json")
        json_default_envs = get_deployment_config(json_file).get_environment("default")
        emails = json_default_envs.get("jobs")[0].get("email_notifications").get("on_failure")

        env_email_value = emails[0]
        preset_email = emails[1]

        assert env_email_value == "test@test.com"
        assert preset_email == "presetEmail@test.com"
        del os.environ["ALERT_EMAIL"]

    def test_json_file_with_env_variables_default_values_with_braces(self):
        """
        JSON:
        max_retries is set to ${MAX_RETRY:3} i.e. with braces

        MAX_RETRY env var will not be set. It should default to 3
        based on config in deployment file
        """
        # MAX_RETRY not set

        json_file = format_path("../deployment-configs/03-json-with-env-vars.json")
        json_default_envs = get_deployment_config(json_file).get_environment("default")
        max_retries = json_default_envs.get("jobs")[0].get("max_retries")

        assert int(max_retries) == 3

    def test_json_file_with_env_variables_default_values_without_braces(self):
        """
        JSON:
        aws_attributes.availability is set to $AVAILABILITY:SPOT i.e. without braces

        AVAILABILITY env var will not be set. It should default to SPOT
        based on config in deployment file
        """
        # AVAILABILITY not set

        json_file = format_path("../deployment-configs/03-json-with-env-vars.json")
        json_default_envs = get_deployment_config(json_file).get_environment("default")
        availability = json_default_envs.get("jobs")[0].get("new_cluster").get("aws_attributes").get("availability")

        assert availability == "SPOT"

    def test_yaml_file_with_env_variables_scalar_type(self):
        """
        YAML: Simple Scalar (key-value) type for timeout_seconds parameter
        """
        # Set TIMEOUT env var
        os.environ["TIMEOUT"] = "100"

        yaml_file = format_path("../deployment-configs/03-yaml-with-env-vars.yaml")
        yaml_default_envs = get_deployment_config(yaml_file).get_environment("default")
        timeout_seconds = yaml_default_envs.get("jobs")[0].get("timeout_seconds")

        assert int(timeout_seconds) == 100
        del os.environ["TIMEOUT"]

    def test_yaml_file_with_env_variables_array_type(self):
        """
        YAML:
        In email_notification.on_failure, one email has been set via env variables
        The other email has been pre-set in deployment.yaml
        """
        # Set ALERT_EMAIL env var
        os.environ["ALERT_EMAIL"] = "test@test.com"

        yaml_file = format_path("../deployment-configs/03-yaml-with-env-vars.yaml")
        yaml_default_envs = get_deployment_config(yaml_file).get_environment("default")
        emails = yaml_default_envs.get("jobs")[0].get("email_notifications").get("on_failure")

        env_email_value = emails[0]
        preset_email = emails[1]

        assert env_email_value == "test@test.com"
        assert preset_email == "presetEmail@test.com"
        del os.environ["ALERT_EMAIL"]

    def test_yaml_file_with_env_variables_default_values(self):
        """
        YAML:
        MAX_RETRY env var will not be set. It should default to 3
        based on config in deployment file
        """
        # MAX_RETRY not set

        yaml_file = format_path("../deployment-configs/03-yaml-with-env-vars.yaml")
        yaml_default_envs = get_deployment_config(yaml_file).get_environment("default")
        max_retries = yaml_default_envs.get("jobs")[0].get("max_retries")

        assert int(max_retries) == 3


class CommonTest(DbxTest):
    def test_update_json(self):
        self.assertRaises(FileNotFoundError, update_json, {"key": "value"}, "/absolutely/not/existent/file/path.json")

    def test_context_lock_file(self):
        self.assertIsNone(ContextLockFile.get_context())

    def update_project_file(self):
        pass


if __name__ == "__main__":
    unittest.main()
