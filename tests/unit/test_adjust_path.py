import glob
import json
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from dbx.commands.deploy import _adjust_job_definitions, FileUploader  # noqa


def format_path(rel_path: str):
    """Format proper path for a given relative path or a glob path"""
    # required for when pytest is NOT ran from within the tests/unit dir.
    return os.path.join(os.path.dirname(__file__), rel_path)


class AdjustPathTest(unittest.TestCase):
    def test_single_node(self):
        glob_path = format_path("../deployment-configs/*.json")

        for file in glob.glob(glob_path):
            if "named-properties" not in file:
                raw_conf = Path(file).read_text()
                deployment_config = json.loads(raw_conf)
                deployment = deployment_config["default"]
                artifact_base_uri = "dbfs:/fake/test"
                requirements_payload = []
                package_requirement = []
                api_client = MagicMock()
                _file_uploader = FileUploader(artifact_base_uri)
                _adjust_job_definitions(
                    deployment["jobs"],
                    requirements_payload,
                    package_requirement,
                    _file_uploader,
                    api_client,
                )

                for job_spec in deployment.get("jobs"):
                    for key, value in job_spec.items():
                        self.assertIsNotNone(value)


if __name__ == "__main__":
    unittest.main()
