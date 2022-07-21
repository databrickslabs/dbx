from typing import Dict, Any
from unittest.mock import MagicMock, patch

import pytest
from databricks_cli.clusters.api import ClusterService
from databricks_cli.instance_pools.api import InstancePoolService

from dbx.api.config_reader import ConfigReader
from dbx.utils.adjuster import adjust_job_definitions
from dbx.utils.dependency_manager import DependencyManager
from dbx.utils.named_properties import NewClusterPropertiesProcessor, WorkloadPropertiesProcessor
from .conftest import get_path_with_relation_to_current_file

samples_root_path = get_path_with_relation_to_current_file("../deployment-configs/")

json_conf = samples_root_path / "05-json-with-named-properties.json"
yaml_conf = samples_root_path / "05-yaml-with-named-properties.yaml"

mtj_json_conf = samples_root_path / "08-json-with-named-properties-mtj.json"
mtj_yaml_conf = samples_root_path / "08-yaml-with-named-properties-mtj.yaml"

json_deployment_conf = ConfigReader(json_conf).get_environment("default")
yaml_deployment_conf = ConfigReader(yaml_conf).get_environment("default")

mtj_json_dep_conf = ConfigReader(mtj_json_conf).get_environment("default")
mtj_yaml_dep_conf = ConfigReader(mtj_yaml_conf).get_environment("default")


def get_job_by_name(src: Dict[str, Any], name: str):
    matched = [j for j in src["jobs"] if j["name"] == name]
    return matched[0]


def test_instance_profile_name_positive():
    job_in_json = get_job_by_name(json_deployment_conf, "named-props-instance-profile-name")
    job_in_yaml = get_job_by_name(yaml_deployment_conf, "named-props-instance-profile-name")

    api_client = MagicMock()
    test_profile_arn = "arn:aws:iam::123456789:instance-profile/some-instance-profile-name"
    api_client.perform_query = MagicMock(
        return_value={"instance_profiles": [{"instance_profile_arn": test_profile_arn}]}
    )

    processor = NewClusterPropertiesProcessor(api_client)
    processor.process(job_in_json["new_cluster"])
    processor.process(job_in_yaml["new_cluster"])

    assert job_in_json["new_cluster"]["aws_attributes"]["instance_profile_arn"] == test_profile_arn
    assert job_in_yaml["new_cluster"]["aws_attributes"]["instance_profile_arn"] == test_profile_arn


def test_instance_profile_name_negative():
    job_in_json = get_job_by_name(json_deployment_conf, "named-props-instance-profile-name")
    job_in_yaml = get_job_by_name(yaml_deployment_conf, "named-props-instance-profile-name")

    api_client = MagicMock()
    api_client.perform_query = MagicMock(
        return_value={
            "instance_profiles": [
                {"instance_profile_arn": "arn:aws:iam::123456789:instance-profile/another-instance-profile-name"}
            ]
        }
    )

    processor = NewClusterPropertiesProcessor(api_client)

    funcs = [
        lambda: processor.process(job_in_json["new_cluster"]),
        lambda: processor.process(job_in_yaml["new_cluster"]),
    ]

    for func in funcs:
        with pytest.raises(Exception):
            func()


def test_instance_pool_name_positive():
    job_in_json = get_job_by_name(json_deployment_conf, "named-props-instance-pool-name")
    job_in_yaml = get_job_by_name(yaml_deployment_conf, "named-props-instance-pool-name")

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

        assert job_in_json["new_cluster"]["instance_pool_id"] == test_pool_id
        assert job_in_yaml["new_cluster"]["instance_pool_id"] == test_pool_id


def test_instance_pool_name_negative():
    job_in_json = get_job_by_name(json_deployment_conf, "named-props-instance-pool-name")
    job_in_yaml = get_job_by_name(yaml_deployment_conf, "named-props-instance-pool-name")

    api_client = MagicMock()

    processor = NewClusterPropertiesProcessor(api_client)

    funcs = [
        lambda: processor.process(job_in_json["new_cluster"]),
        lambda: processor.process(job_in_yaml["new_cluster"]),
    ]

    for func in funcs:
        with pytest.raises(Exception):
            func()


def test_existing_cluster_name_positive():
    job_in_json = get_job_by_name(json_deployment_conf, "named-props-existing-cluster-name")
    job_in_yaml = get_job_by_name(yaml_deployment_conf, "named-props-existing-cluster-name")

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

        assert job_in_yaml["existing_cluster_id"] == test_existing_cluster_id
        assert job_in_json["existing_cluster_id"] == test_existing_cluster_id


def test_existing_cluster_name_negative():
    job1 = get_job_by_name(json_deployment_conf, "named-props-existing-cluster-name")
    api_client = MagicMock()

    processor = WorkloadPropertiesProcessor(api_client)

    with pytest.raises(Exception):
        processor.process(job1)


def test_mtj_named_positive():
    file_uploader = MagicMock()
    api_client = MagicMock()
    test_profile_arn = "arn:aws:iam::123456789:instance-profile/some-instance-profile-name"

    dm = DependencyManager(global_no_package=False, no_rebuild=True, requirements_file=None)

    api_client.perform_query = MagicMock(
        return_value={"instance_profiles": [{"instance_profile_arn": test_profile_arn}]}
    )

    sample_reference = {"whl": "path/to/some/file"}
    dm._core_package_reference = sample_reference

    for deployment_conf in [mtj_json_dep_conf, mtj_yaml_dep_conf]:
        jobs = deployment_conf["jobs"]

        adjust_job_definitions(jobs=jobs, dependency_manager=dm, file_uploader=file_uploader, api_client=api_client)

        assert jobs[0]["job_clusters"][0]["new_cluster"]["aws_attributes"]["instance_profile_arn"] is not None
        assert jobs[0]["tasks"][0]["libraries"] == []
        assert jobs[0]["tasks"][1]["libraries"] == [sample_reference]
