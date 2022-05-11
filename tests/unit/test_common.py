import os
import unittest
from unittest import mock

from dbx.utils.common import get_deployment_config


def format_path(rel_path: str):
    """Format proper path for a given relative path or a glob path"""
    # required for when pytest is NOT ran from within the tests/unit dir.
    return os.path.join(os.path.dirname(__file__), rel_path)


class CommonUnitTest(unittest.TestCase):
    def test_all_file_formats_can_be_read(self):
        json_file = format_path("../deployment-configs/01-json-test.json")
        yaml_file = format_path("../deployment-configs/01-yaml-test.yaml")
        jinja_json_file = format_path("../deployment-configs/01-jinja-test.json.j2")
        jinja_yaml_file = format_path("../deployment-configs/01-jinja-test.yaml.j2")

        json_default_envs = get_deployment_config(json_file).get_all_environment_names()
        yaml_default_envs = get_deployment_config(yaml_file).get_all_environment_names()
        jinja_json_default_envs = get_deployment_config(jinja_json_file).get_all_environment_names()
        jinja_yaml_default_envs = get_deployment_config(jinja_yaml_file).get_all_environment_names()

        assert json_default_envs == yaml_default_envs == jinja_json_default_envs == jinja_yaml_default_envs

    def test_all_file_formats_contents_match(self):
        json_file = format_path("../deployment-configs/01-json-test.json")
        yaml_file = format_path("../deployment-configs/01-yaml-test.yaml")
        jinja_json_file = format_path("../deployment-configs/01-jinja-test.json.j2")
        jinja_yaml_file = format_path("../deployment-configs/01-jinja-test.yaml.j2")

        json_default_env = get_deployment_config(json_file).get_environment("default")
        yaml_default_env = get_deployment_config(yaml_file).get_environment("default")
        jinja_json_default_env = get_deployment_config(jinja_json_file).get_environment("default")
        jinja_yaml_default_env = get_deployment_config(jinja_yaml_file).get_environment("default")

        assert yaml_default_env == json_default_env == jinja_json_default_env == jinja_yaml_default_env

    def test_all_file_formats_variables_match(self):
        json_file = format_path("../deployment-configs/02-json-with-vars-test.json")
        yaml_file = format_path("../deployment-configs/02-yaml-with-vars-test.yaml")
        jinja_json_file = format_path("../deployment-configs/02-jinja-with-vars-test.json.j2")
        jinja_yaml_file = format_path("../deployment-configs/02-jinja-with-vars-test.yaml.j2")

        json_default_env = get_deployment_config(json_file).get_environment("default")
        yaml_default_env = get_deployment_config(yaml_file).get_environment("default")
        jinja_json_default_env = get_deployment_config(jinja_json_file).get_environment("default")
        jinja_yaml_default_env = get_deployment_config(jinja_yaml_file).get_environment("default")

        assert yaml_default_env == json_default_env == jinja_json_default_env == jinja_yaml_default_env

    @mock.patch.dict(os.environ, {"TIMEOUT": "100"}, clear=True)
    def test_json_file_with_env_variables_scalar_type(self):
        """
        JSON: Simple Scalar (key-value) type for timeout_seconds parameter
        """
        json_file = format_path("../deployment-configs/04-json-with-env-vars.json")
        json_default_envs = get_deployment_config(json_file).get_environment("default")
        timeout_seconds = json_default_envs.get("jobs")[0].get("timeout_seconds")

        self.assertEqual(int(timeout_seconds), 100)

    @mock.patch.dict(os.environ, {"ALERT_EMAIL": "test@test.com"}, clear=True)
    def test_json_file_with_env_variables_array_type(self):
        """
        JSON:
        In email_notification.on_failure, one email has been set via env variables
        The other email has been pre-set in deployment.yaml
        """
        json_file = format_path("../deployment-configs/04-json-with-env-vars.json")
        json_default_envs = get_deployment_config(json_file).get_environment("default")
        emails = json_default_envs.get("jobs")[0].get("email_notifications").get("on_failure")

        env_email_value = emails[0]
        preset_email = emails[1]

        self.assertEqual(env_email_value, "test@test.com")
        self.assertEqual(preset_email, "presetEmail@test.com")

    def test_json_file_with_env_variables_default_values_with_braces(self):
        """
        JSON:
        max_retries is set to ${MAX_RETRY:3} i.e. with braces

        MAX_RETRY env var will not be set. It should default to 3
        based on config in deployment file
        """
        # MAX_RETRY not set

        json_file = format_path("../deployment-configs/04-json-with-env-vars.json")
        json_default_envs = get_deployment_config(json_file).get_environment("default")
        max_retries = json_default_envs.get("jobs")[0].get("max_retries")

        self.assertEqual(int(max_retries), 3)

    def test_json_file_with_env_variables_default_values_without_braces(self):
        """
        JSON:
        aws_attributes.availability is set to $AVAILABILITY:SPOT i.e. without braces

        AVAILABILITY env var will not be set. It should default to SPOT
        based on config in deployment file
        """
        # AVAILABILITY not set

        json_file = format_path("../deployment-configs/04-json-with-env-vars.json")
        json_default_envs = get_deployment_config(json_file).get_environment("default")
        availability = json_default_envs.get("jobs")[0].get("new_cluster").get("aws_attributes").get("availability")

        self.assertEqual(availability, "SPOT")

    @mock.patch.dict(os.environ, {"TIMEOUT": "100"}, clear=True)
    def test_yaml_file_with_env_variables_scalar_type(self):
        """
        YAML: Simple Scalar (key-value) type for timeout_seconds parameter
        """
        yaml_file = format_path("../deployment-configs/04-yaml-with-env-vars.yaml")
        yaml_default_envs = get_deployment_config(yaml_file).get_environment("default")
        timeout_seconds = yaml_default_envs.get("jobs")[0].get("timeout_seconds")

        self.assertEqual(int(timeout_seconds), 100)

    @mock.patch.dict(os.environ, {"ALERT_EMAIL": "test@test.com"}, clear=True)
    def test_yaml_file_with_env_variables_array_type(self):
        """
        YAML:
        In email_notification.on_failure, one email has been set via env variables
        The other email has been pre-set in deployment.yaml
        """
        yaml_file = format_path("../deployment-configs/04-yaml-with-env-vars.yaml")
        yaml_default_envs = get_deployment_config(yaml_file).get_environment("default")
        emails = yaml_default_envs.get("jobs")[0].get("email_notifications").get("on_failure")

        env_email_value = emails[0]
        preset_email = emails[1]

        self.assertEqual(env_email_value, "test@test.com")
        self.assertEqual(preset_email, "presetEmail@test.com")

    def test_yaml_file_with_env_variables_default_values(self):
        """
        YAML:
        MAX_RETRY env var will not be set. It should default to 3
        based on config in deployment file
        """
        # MAX_RETRY not set

        yaml_file = format_path("../deployment-configs/04-yaml-with-env-vars.yaml")
        yaml_default_envs = get_deployment_config(yaml_file).get_environment("default")
        max_retries = yaml_default_envs.get("jobs")[0].get("max_retries")

        self.assertEqual(int(max_retries), 3)

    @mock.patch.dict(os.environ, {"TIMEOUT": "100"}, clear=True)
    def test_jinja_files_with_env_variables_scalar_type(self):
        """
        JINJA2: Simple Scalar (key-value) type for timeout_seconds parameter
        """
        json_j2_file = format_path("../deployment-configs/04-jinja-with-env-vars.json.j2")
        yaml_j2_file = format_path("../deployment-configs/04-jinja-with-env-vars.yaml.j2")

        json_default_envs = get_deployment_config(json_j2_file).get_environment("default")
        yaml_default_envs = get_deployment_config(yaml_j2_file).get_environment("default")

        json_timeout_seconds = json_default_envs.get("jobs")[0].get("timeout_seconds")
        yaml_timeout_seconds = yaml_default_envs.get("jobs")[0].get("timeout_seconds")

        self.assertEqual(int(json_timeout_seconds), 100)
        self.assertEqual(int(yaml_timeout_seconds), 100)

    @mock.patch.dict(os.environ, {"ALERT_EMAIL": "test@test.com"}, clear=True)
    def test_jinja_files_with_env_variables_array_type(self):
        """
        JINJA2: In email_notification.on_failure, the first email has been set via env variables
        """
        json_j2_file = format_path("../deployment-configs/04-jinja-with-env-vars.json.j2")
        yaml_j2_file = format_path("../deployment-configs/04-jinja-with-env-vars.yaml.j2")
        json_default_envs = get_deployment_config(json_j2_file).get_environment("default")
        yaml_default_envs = get_deployment_config(yaml_j2_file).get_environment("default")

        json_emails = json_default_envs.get("jobs")[0].get("email_notifications").get("on_failure")
        yaml_emails = yaml_default_envs.get("jobs")[0].get("email_notifications").get("on_failure")

        self.assertEqual(json_emails, yaml_emails)
        self.assertEqual(json_emails[0], "test@test.com")

    def test_jinja_file_with_env_variables_default_values(self):
        """
        JINJA:
        max_retries is set to {{ MAX_RETRY | default(3) }};
        new_cluster.aws_attributes.availability is set to {{ AVAILABILITY | default('SPOT') }}.

        MAX_RETRY and AVAILABILITY env vars will not be set. They should default to 3 and 'SPOT'
        based on config in deployment file
        """
        json_j2_file = format_path("../deployment-configs/04-jinja-with-env-vars.json.j2")
        yaml_j2_file = format_path("../deployment-configs/04-jinja-with-env-vars.yaml.j2")
        json_default_envs = get_deployment_config(json_j2_file).get_environment("default")
        yaml_default_envs = get_deployment_config(yaml_j2_file).get_environment("default")

        json_max_retries = json_default_envs.get("jobs")[0].get("max_retries")
        yaml_max_retries = yaml_default_envs.get("jobs")[0].get("max_retries")
        json_avail = json_default_envs.get("jobs")[0].get("new_cluster").get("aws_attributes").get("availability")
        yaml_avail = yaml_default_envs.get("jobs")[0].get("new_cluster").get("aws_attributes").get("availability")

        self.assertEqual(int(json_max_retries), int(yaml_max_retries))
        self.assertEqual(int(json_max_retries), 3)
        self.assertEqual(json_avail, yaml_avail)
        self.assertEqual(json_avail, "SPOT")

    @mock.patch.dict(os.environ, {"ENVIRONMENT": "PRODUCTION"}, clear=True)
    def test_jinja_files_with_env_variables_logic_1(self):
        """
        JINJA:
        - max_retries is set to {{ MAX_RETRY | default(-1) }} if (ENVIRONMENT.lower() == "production"),
          {{ MAX_RETRY | default(3) }} otherwise.
        - email_notifications are only set if (ENVIRONMENT.lower() == "production").

        ENVIRONMENT is set to "production", so MAX_RETRY and EMAIL_NOTIFICATIONS env vars
        should correspond to the ENVIRONMENT=production values.
        Also testing filters like .lower() are working correctly.
        """
        json_j2_file = format_path("../deployment-configs/06-jinja-with-logic.json.j2")
        yaml_j2_file = format_path("../deployment-configs/06-jinja-with-logic.yaml.j2")
        json_default_envs = get_deployment_config(json_j2_file).get_environment("default")
        yaml_default_envs = get_deployment_config(yaml_j2_file).get_environment("default")

        json_max_retries = json_default_envs.get("jobs")[0].get("max_retries")
        yaml_max_retries = yaml_default_envs.get("jobs")[0].get("max_retries")
        json_emails = json_default_envs.get("jobs")[0].get("email_notifications").get("on_failure")
        yaml_emails = yaml_default_envs.get("jobs")[0].get("email_notifications").get("on_failure")

        self.assertEqual(int(json_max_retries), -1)
        self.assertEqual(int(yaml_max_retries), -1)
        self.assertEqual(json_emails[0], "presetEmail@test.com")
        self.assertEqual(yaml_emails[0], "presetEmail@test.com")

    @mock.patch.dict(os.environ, {"ENVIRONMENT": "test"}, clear=True)
    def test_jinja_files_with_env_variables_logic_2(self):
        """
        JINJA:
        - max_retries is set to {{ MAX_RETRY | default(-1) }} if (ENVIRONMENT == "production"),
          {{ MAX_RETRY | default(3) }} otherwise.
        - email_notifications are only set if (ENVIRONMENT == "production").

        ENVIRONMENT is set to "test", so MAX_RETRY and EMAIL_NOTIFICATIONS env vars
        should correspond to the ENVIRONMENT != production values.
        """
        json_j2_file = format_path("../deployment-configs/06-jinja-with-logic.json.j2")
        yaml_j2_file = format_path("../deployment-configs/06-jinja-with-logic.yaml.j2")
        json_default_envs = get_deployment_config(json_j2_file).get_environment("default")
        yaml_default_envs = get_deployment_config(yaml_j2_file).get_environment("default")

        json_max_retries = json_default_envs.get("jobs")[0].get("max_retries")
        yaml_max_retries = yaml_default_envs.get("jobs")[0].get("max_retries")
        json_emails = json_default_envs.get("jobs")[0].get("email_notifications")
        yaml_emails = yaml_default_envs.get("jobs")[0].get("email_notifications")

        self.assertEqual(int(json_max_retries), 3)
        self.assertEqual(int(yaml_max_retries), 3)
        self.assertEqual(json_emails, None)
        self.assertEqual(yaml_emails, None)


if __name__ == "__main__":
    unittest.main()
