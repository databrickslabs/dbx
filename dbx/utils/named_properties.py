"""
Databricks Jobs API supports various arguments with _id-based API, but this is really quirky for end users.
In particular, this code supports the following resolutions:
1. Job and task-name resolutions:
    - existing_cluster_name
    - for new_cluster on job name:
        - aws_attributes.instance_profile_id
        - new_cluster.instance_pool_name
        - new_cluster.driver_instance_pool_name
        - new_cluster.aws_attributes.instance_profile_name
2. Multitask-jobs job_clusters properties
3. policy_name on the new_cluster structure
"""
import abc
import collections.abc
import json
from typing import Dict, Any

from databricks_cli.sdk import ApiClient, InstancePoolService, PolicyService

from dbx.utils.common import _preprocess_cluster_args
from dbx.utils import dbx_echo
from dbx.utils.policy_parser import PolicyParser


class AbstractProcessor(abc.ABC):
    def __init__(self, api_client: ApiClient):
        self._api_client = api_client

    @abc.abstractmethod
    def process(self, object_reference: Dict[str, Any]):
        pass


class PolicyNameProcessor(AbstractProcessor):
    def process(self, object_reference: Dict[str, Any]):
        policy_name = object_reference.get("policy_name")

        if policy_name:
            dbx_echo(f"Processing policy name {policy_name}")
            policy_spec = self._preprocess_policy_name(policy_name)
            policy = json.loads(policy_spec["definition"])
            policy_props = PolicyParser(policy).parse()
            self._deep_update(object_reference, policy_props, policy_name)
            object_reference["policy_id"] = policy_spec["policy_id"]

    @staticmethod
    def _deep_update(d: Dict, u: collections.abc.Mapping, policy_name: str) -> Dict:
        for k, v in u.items():
            if isinstance(v, collections.abc.Mapping):
                d[k] = PolicyNameProcessor._deep_update(d.get(k, {}), v, policy_name)
            else:
                # if the key is already provided in deployment configuration, we need to verify the value
                # if value exists, we verify that it's the same as in the policy
                existing_value = d.get(k)
                if existing_value:
                    if existing_value != v:
                        raise Exception(
                            f"For key {k} there is a value in the cluster definition: {existing_value} \n"
                            f"However this value is fixed in the policy {policy_name} and shall be equal to: {v}"
                        )
                d[k] = v
        return d

    def _preprocess_policy_name(self, policy_name: str):
        policies = PolicyService(self._api_client).list_policies().get("policies", [])
        found_policies = [p for p in policies if p["name"] == policy_name]

        if not found_policies:
            raise Exception(f"Policy {policy_name} not found")

        if len(found_policies) > 1:
            raise Exception(f"Policy with name {policy_name} is not unique. Please make unique names for policies.")

        policy_spec = found_policies[0]
        return policy_spec


class WorkloadPropertiesProcessor(AbstractProcessor):
    def process(self, object_reference: Dict[str, Any]):
        self._preprocess_existing_cluster_name(object_reference)

    def _preprocess_existing_cluster_name(self, object_reference: Dict[str, Any]):
        existing_cluster_name = object_reference.get("existing_cluster_name")

        if existing_cluster_name:
            dbx_echo("Named parameter existing_cluster_name is provided, looking for it's id")
            existing_cluster_id = _preprocess_cluster_args(self._api_client, existing_cluster_name, None)
            object_reference["existing_cluster_id"] = existing_cluster_id


class NewClusterPropertiesProcessor(AbstractProcessor):
    def process(self, object_reference: Dict[str, Any]):
        self._preprocess_instance_profile_name(object_reference)
        self._preprocess_driver_instance_pool_name(object_reference)
        self._preprocess_instance_pool_name(object_reference)

    @staticmethod
    def _name_from_profile(profile_def) -> str:
        return profile_def.get("instance_profile_arn").split("/")[-1]

    def _preprocess_instance_profile_name(self, object_reference: Dict[str, Any]):
        instance_profile_name = object_reference.get("aws_attributes", {}).get("instance_profile_name")

        if instance_profile_name:
            dbx_echo("Named parameter instance_profile_name is provided, looking for it's id")
            all_instance_profiles = self._api_client.perform_query("get", "/instance-profiles/list").get(
                "instance_profiles", []
            )
            instance_profile_names = [self._name_from_profile(p) for p in all_instance_profiles]
            matching_profiles = [
                p for p in all_instance_profiles if self._name_from_profile(p) == instance_profile_name
            ]

            if not matching_profiles:
                raise Exception(
                    f"No instance profile with name {instance_profile_name} found."
                    f"Available instance profiles are: {instance_profile_names}"
                )

            if len(matching_profiles) > 1:
                raise Exception(
                    f"Found multiple instance profiles with name {instance_profile_name}"
                    f"Please provide unique names for the instance profiles."
                )

            object_reference["aws_attributes"]["instance_profile_arn"] = matching_profiles[0]["instance_profile_arn"]

    def _preprocess_driver_instance_pool_name(self, object_reference: Dict[str, Any]):
        self._generic_instance_pool_name_preprocessor(
            object_reference, "driver_instance_pool_name", "instance_pool_id", "driver_instance_pool_id"
        )

    def _preprocess_instance_pool_name(self, object_reference: Dict[str, Any]):
        self._generic_instance_pool_name_preprocessor(
            object_reference, "instance_pool_name", "instance_pool_id", "instance_pool_id"
        )

    def _generic_instance_pool_name_preprocessor(
        self, object_reference: Dict[str, Any], named_parameter, search_id, property_name
    ):
        instance_pool_name = object_reference.get(named_parameter)

        if instance_pool_name:
            dbx_echo(f"Named parameter {named_parameter} is provided, looking for its id")
            all_pools = InstancePoolService(self._api_client).list_instance_pools().get("instance_pools", [])
            instance_pool_names = [p.get("instance_pool_name") for p in all_pools]
            matching_pools = [p for p in all_pools if p["instance_pool_name"] == instance_pool_name]

            if not matching_pools:
                raise Exception(
                    f"No instance pool with name {instance_pool_name} found, available pools: {instance_pool_names}"
                )

            if len(matching_pools) > 1:
                raise Exception(
                    f"Found multiple pools with name {instance_pool_name}, please provide unique names for the pools"
                )

            object_reference[property_name] = matching_pools[0][search_id]
