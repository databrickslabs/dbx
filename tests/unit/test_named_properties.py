import unittest
import json

import pathlib
from typing import Dict, Any
from unittest.mock import MagicMock, patch

from databricks_cli.instance_pools.api import InstancePoolService
from databricks_cli.clusters.api import ClusterService

from dbx.commands.deploy import NamedPropertiesProcessor
from dbx.utils.common import YamlDeploymentConfig
from .test_common import format_path


class NamedPropertiesProcessorTest(unittest.TestCase):
    samples_root_path = pathlib.Path(format_path("../deployment-configs/"))

    json_conf = pathlib.Path(samples_root_path) / "05-json-with-named-properties.json"
    yaml_conf = pathlib.Path(samples_root_path) / "05-yaml-with-named-properties.yaml"

    json_deployment_conf = json.loads(json_conf.read_text()).get("default")
    yaml_deployment_conf = YamlDeploymentConfig(yaml_conf).get_environment("default")

    @staticmethod
    def _get_job_by_name(src: Dict[str, Any], name: str):
        matched = [j for j in src["jobs"] if j["name"] == name]
        return matched[0]

    def test_instance_profile_name_positive(self):
        job_in_json = self._get_job_by_name(self.json_deployment_conf, "named-props-instance-profile-name")
        job_in_yaml = self._get_job_by_name(self.yaml_deployment_conf, "named-props-instance-profile-name")

        api_client = MagicMock()
        test_profile_arn = "arn:aws:iam::123456789:instance-profile/some-instance-profile-name"
        api_client.perform_query = MagicMock(
            return_value={"instance_profiles": [{"instance_profile_arn": test_profile_arn}]}
        )

        NamedPropertiesProcessor(job_in_json, api_client).preprocess()
        NamedPropertiesProcessor(job_in_yaml, api_client).preprocess()

        self.assertEqual(job_in_json["new_cluster"]["aws_attributes"]["instance_profile_arn"], test_profile_arn)
        self.assertEqual(job_in_yaml["new_cluster"]["aws_attributes"]["instance_profile_arn"], test_profile_arn)

    def test_instance_profile_name_negative(self):
        job_in_json = self._get_job_by_name(self.json_deployment_conf, "named-props-instance-profile-name")
        job_in_yaml = self._get_job_by_name(self.yaml_deployment_conf, "named-props-instance-profile-name")

        api_client = MagicMock()
        api_client.perform_query = MagicMock(
            return_value={
                "instance_profiles": [
                    {"instance_profile_arn": "arn:aws:iam::123456789:instance-profile/another-instance-profile-name"}
                ]
            }
        )
        self.assertRaises(Exception, NamedPropertiesProcessor(job_in_json, api_client).preprocess)
        self.assertRaises(Exception, NamedPropertiesProcessor(job_in_yaml, api_client).preprocess)

    def test_instance_pool_name_positive(self):
        job_in_json = self._get_job_by_name(self.json_deployment_conf, "named-props-instance-pool-name")
        job_in_yaml = self._get_job_by_name(self.yaml_deployment_conf, "named-props-instance-pool-name")

        api_client = MagicMock()
        test_pool_id = "aaa-bbb-000-ccc"

        with patch.object(
            InstancePoolService,
            "list_instance_pools",
            return_value={
                "instance_pools": [{"instance_pool_name": "some-instance-pool-name", "instance_pool_id": test_pool_id}]
            },
        ):
            NamedPropertiesProcessor(job_in_json, api_client).preprocess()
            NamedPropertiesProcessor(job_in_yaml, api_client).preprocess()

            self.assertEqual(job_in_json["new_cluster"]["instance_pool_id"], test_pool_id)
            self.assertEqual(job_in_yaml["new_cluster"]["instance_pool_id"], test_pool_id)

    def test_instance_pool_name_negative(self):
        job_in_json = self._get_job_by_name(self.json_deployment_conf, "named-props-instance-pool-name")
        job_in_yaml = self._get_job_by_name(self.yaml_deployment_conf, "named-props-instance-pool-name")

        api_client = MagicMock()

        self.assertRaises(Exception, NamedPropertiesProcessor(job_in_json, api_client).preprocess)
        self.assertRaises(Exception, NamedPropertiesProcessor(job_in_yaml, api_client).preprocess)

    def test_existing_cluster_name_positive(self):
        job_in_json = self._get_job_by_name(self.json_deployment_conf, "named-props-existing-cluster-name")
        job_in_yaml = self._get_job_by_name(self.yaml_deployment_conf, "named-props-existing-cluster-name")

        api_client = MagicMock()
        test_existing_cluster_id = "aaa-bbb-000-ccc"
        with patch.object(
            ClusterService,
            "list_clusters",
            return_value={"clusters": [{"cluster_name": "some-cluster", "cluster_id": test_existing_cluster_id}]},
        ):
            NamedPropertiesProcessor(job_in_json, api_client).preprocess()
            NamedPropertiesProcessor(job_in_yaml, api_client).preprocess()
            self.assertEqual(job_in_yaml["existing_cluster_id"], test_existing_cluster_id)
            self.assertEqual(job_in_json["existing_cluster_id"], test_existing_cluster_id)

    def test_existing_cluster_name_negative(self):
        job1 = self._get_job_by_name(self.json_deployment_conf, "named-props-instance-pool-name")
        api_client = MagicMock()
        self.assertRaises(Exception, NamedPropertiesProcessor(job1, api_client).preprocess)


if __name__ == "__main__":
    unittest.main()
