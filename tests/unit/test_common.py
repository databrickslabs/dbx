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


class CommonTest(DbxTest):
    def test_update_json(self):
        self.assertRaises(FileNotFoundError, update_json, {"key": "value"}, "/absolutely/not/existent/file/path.json")

    def test_context_lock_file(self):
        self.assertIsNone(ContextLockFile.get_context())

    def update_project_file(self):
        pass


if __name__ == "__main__":
    unittest.main()
