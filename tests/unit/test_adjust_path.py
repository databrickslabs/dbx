import unittest
from dbx.commands.deploy import _adjust_job_definitions, FileUploader  # noqa
import json
from unittest.mock import MagicMock
import glob
from pathlib import Path


class AdjustPathTest(unittest.TestCase):
    def test_single_node(self):
        for file in glob.glob("../deployment-configs/*.json"):
            raw_conf = Path(file).read_text()
            deployment = {"jobs": [json.loads(raw_conf)]}
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
            )

            for job_spec in deployment.get("jobs"):
                for key, value in job_spec.items():
                    self.assertIsNotNone(value)


if __name__ == "__main__":
    unittest.main()
