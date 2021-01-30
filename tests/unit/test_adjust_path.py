import unittest
from dbx.commands.deploy import _adjust_job_definitions, FileUploader
import json
from unittest.mock import MagicMock


class AdjustPathTest(unittest.TestCase):
    def test_single_node(self):
        dep_file_content = """
        {
                "name": "sample-test-job",
                "new_cluster": {
                    "spark_version": "7.3.x-scala2.12",
                    "spark_conf": {
                        "spark.databricks.delta.preview.enabled": "true",
                        "spark.master": "local[*]",
                        "spark.databricks.cluster.profile": "singleNode"
                    },
                    "node_type_id": "Standard_DS3_v2",
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                        "job": "single-sample"
                    },
                    "enable_elastic_disk": true,
                    "init_scripts": [
                        {
                            "dbfs": {
                                "destination": "dbfs:/monitoring/datadog_install_driver_only.sh"
                            }
                        }
                    ],
                    "azure_attributes": {
                        "availability": "ON_DEMAND_AZURE"
                    },
                    "num_workers": 0
                },
                "libraries": [],
                "email_notifications": {
                    "on_start": [],
                    "on_success": [],
                    "on_failure": []
                },
                "max_retries": 0,
                "spark_python_task": {
                    "python_file": "project_name/jobs/sample/entrypoint.py",
                    "parameters": [
                        "--conf-file",
                        "conf/test/sample.json"
                    ]
                }
            }
        """
        deployment = {"jobs": [json.loads(dep_file_content)]}
        artifact_base_uri = "dbfs:/fake/test"
        requirements_payload = []
        package_requirement = []
        api_client = MagicMock()
        _file_uploader = FileUploader(api_client)
        _adjust_job_definitions(deployment["jobs"], artifact_base_uri,
                                requirements_payload, package_requirement, _file_uploader)


if __name__ == '__main__':
    unittest.main()
