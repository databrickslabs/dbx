import unittest
from dbx.commands.deploy import _adjust_job_definitions, FileUploader  # noqa
import json
from unittest.mock import MagicMock
import glob
from pathlib import Path
import os


class AdjustPathTest(unittest.TestCase):
    def test_single_node(self):
        # required for when pytest is NOT ran from within the tests/unit dir.
        glob_path = os.path.join(os.path.dirname(__file__), "../deployment-configs/*.json")

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


if __name__ == "__main__":
    unittest.main()
