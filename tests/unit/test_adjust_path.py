from pydash import py_
import unittest
from dbx.commands.deploy import _adjust_job_definitions, FileUploader  # noqa
import json
from unittest.mock import MagicMock
import glob
from pathlib import Path
import os


def format_path(rel_path: str):
    """Format proper path for a given relative path or a glob path"""
    # required for when pytest is NOT ran from within the tests/unit dir.
    return os.path.join(os.path.dirname(__file__), rel_path)


class AdjustPathTest(unittest.TestCase):
    def test_single_node(self):
        glob_path = format_path("../deployment-configs/*.json")

        for file in glob.glob(glob_path):
            raw_conf = Path(file).read_text()
            deployment_config = json.loads(raw_conf)
            deployment = deployment_config["default"]
            artifact_base_uri = "dbfs:/fake/test"
            requirements_payload = []
            package_requirement = []
            api_client = MagicMock()
            _file_uploader = FileUploader(api_client)
            _adjust_job_definitions(
                deployment["jobs"],
                artifact_base_uri,
                requirements_payload,
                package_requirement,
                _file_uploader,
                api_client,
            )

            for job_spec in deployment.get("jobs"):
                for key, value in job_spec.items():
                    self.assertIsNotNone(value)


class AwsAdjustPathTest(unittest.TestCase):
    def test_that_upstream_files_are_checked_and_paths_are_properly_re_written(self):
        # setup
        file_path = format_path("../deployment-configs/aws-example.json")
        raw_conf = Path(file_path).read_text()
        deployment_config = json.loads(raw_conf)
        deployment = deployment_config["default"]
        artifact_base_uri = "dbfs:/fake/test"
        requirements_payload = []
        package_requirement = []
        api_client = MagicMock()
        _file_uploader = FileUploader(api_client)

        # function call
        _adjust_job_definitions(
            deployment["jobs"],
            artifact_base_uri,
            requirements_payload,
            package_requirement,
            _file_uploader,
            api_client,
        )

        # tests
        api_client.perform_query.assert_called_once()
        api_client.perform_query.assert_called_once_with(
            "GET",
            "/dbfs/get-status",
            data={"path": "dbfs:/fake/test/tests/deployment-configs/placeholder.py"},
            headers=None,
        )
        assert (
            py_.get(deployment, "jobs.[0].spark_python_task.python_file")
            == "dbfs:/fake/test/tests/deployment-configs/placeholder.py"
        )


if __name__ == "__main__":
    unittest.main()
