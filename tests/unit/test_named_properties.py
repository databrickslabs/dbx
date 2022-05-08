import unittest


from pathlib import Path
from typing import Dict, Any
from unittest.mock import MagicMock, patch

from databricks_cli.instance_pools.api import InstancePoolService
from databricks_cli.clusters.api import ClusterService

from dbx.utils.adjuster import adjust_job_definitions
from dbx.utils.dependency_manager import DependencyManager
from dbx.utils.named_properties import NewClusterPropertiesProcessor, WorkloadPropertiesProcessor
from dbx.utils.common import YamlDeploymentConfig
from dbx.utils.json import JsonUtils
from .test_common import format_path


class NamedPropertiesProcessorTest(unittest.TestCase):
    samples_root_path = Path(format_path("../deployment-configs/"))

    json_conf = samples_root_path / "05-json-with-named-properties.json"
    yaml_conf = samples_root_path / "05-yaml-with-named-properties.yaml"

    mtj_json_conf = samples_root_path / "08-json-with-named-properties-mtj.json"
    mtj_yaml_conf = samples_root_path / "08-yaml-with-named-properties-mtj.yaml"

    json_deployment_conf = JsonUtils.read(json_conf).get("default")
    yaml_deployment_conf = YamlDeploymentConfig(yaml_conf).get_environment("default")

    mtj_json_dep_conf = JsonUtils.read(mtj_json_conf).get("default")
    mtj_yaml_dep_conf = YamlDeploymentConfig(mtj_yaml_conf).get_environment("default")

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

        processor = NewClusterPropertiesProcessor(api_client)
        processor.process(job_in_json["new_cluster"])
        processor.process(job_in_yaml["new_cluster"])

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

        processor = NewClusterPropertiesProcessor(api_client)
        self.assertRaises(Exception, processor.process, job_in_json["new_cluster"])
        self.assertRaises(Exception, processor.process, job_in_yaml["new_cluster"])

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
            processor = NewClusterPropertiesProcessor(api_client)
            processor.process(job_in_json["new_cluster"])
            processor.process(job_in_yaml["new_cluster"])

            self.assertEqual(job_in_json["new_cluster"]["instance_pool_id"], test_pool_id)
            self.assertEqual(job_in_yaml["new_cluster"]["instance_pool_id"], test_pool_id)

    def test_instance_pool_name_negative(self):
        job_in_json = self._get_job_by_name(self.json_deployment_conf, "named-props-instance-pool-name")
        job_in_yaml = self._get_job_by_name(self.yaml_deployment_conf, "named-props-instance-pool-name")

        api_client = MagicMock()

        processor = NewClusterPropertiesProcessor(api_client)

        self.assertRaises(Exception, processor.process, job_in_json["new_cluster"])
        self.assertRaises(Exception, processor.process, job_in_yaml["new_cluster"])

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
            processor = WorkloadPropertiesProcessor(api_client)
            processor.process(job_in_json)
            processor.process(job_in_yaml)

            self.assertEqual(job_in_yaml["existing_cluster_id"], test_existing_cluster_id)
            self.assertEqual(job_in_json["existing_cluster_id"], test_existing_cluster_id)

    def test_existing_cluster_name_negative(self):
        job1 = self._get_job_by_name(self.json_deployment_conf, "named-props-existing-cluster-name")
        api_client = MagicMock()

        processor = WorkloadPropertiesProcessor(api_client)

        self.assertRaises(Exception, processor.process, job1)

    def test_mtj_named_positive(self):
        file_uploader = MagicMock()
        api_client = MagicMock()
        test_profile_arn = "arn:aws:iam::123456789:instance-profile/some-instance-profile-name"

        dm = DependencyManager(global_no_package=False, no_rebuild=True, strict_adjustment=True, requirements_file=None)

        api_client.perform_query = MagicMock(
            return_value={"instance_profiles": [{"instance_profile_arn": test_profile_arn}]}
        )

        sample_reference = {"whl": "path/to/some/file"}
        dm._core_package_reference = sample_reference

        for deployment_conf in [self.mtj_json_dep_conf, self.mtj_yaml_dep_conf]:
            jobs = deployment_conf["jobs"]

            adjust_job_definitions(jobs=jobs, dependency_manager=dm, file_uploader=file_uploader, api_client=api_client)

            self.assertIsNotNone(jobs[0]["job_clusters"][0]["new_cluster"]["aws_attributes"]["instance_profile_arn"])
            self.assertEqual(jobs[0]["tasks"][0]["libraries"], [])
            self.assertEqual(jobs[0]["tasks"][1]["libraries"], [sample_reference])


if __name__ == "__main__":
    unittest.main()
