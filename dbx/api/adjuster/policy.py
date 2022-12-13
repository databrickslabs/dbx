import json
from collections import defaultdict
from collections.abc import Mapping
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Union

from databricks_cli.cluster_policies.api import PolicyService

from dbx.api.adjuster.mixins.base import ApiClientMixin
from dbx.models.workflow.common.flexible import FlexibleModel
from dbx.models.workflow.common.new_cluster import NewCluster


class Policy(FlexibleModel):
    policy_id: str
    name: str
    definition: str
    description: Optional[str]


class PoliciesResponse(FlexibleModel):
    policies: List[Policy]


class PolicyAdjuster(ApiClientMixin):
    """
    This policy parser is based on:
    - API Doc: policy parser is based on API doc https://docs.databricks.com/dev-tools/api/latest/policies.html
    - Policy definition docs:
        - AWS: https://docs.databricks.com/administration-guide/clusters/policies.html#cluster-policy-attribute-paths
        - Azure: https://docs.microsoft.com/en-us/azure/databricks/administration-guide/clusters/policies
        - GCP: Cluster policies were not supported at the moment of 0.1.3 release.
    Please note that only "fixed" values will be automatically added to the job definition.
    """

    def _adjust_policy_ref(self, cluster: NewCluster):
        policy_service = PolicyService(self.api_client)
        policy = self._get_policy(policy_service, cluster.policy_name, cluster.policy_id)
        traversed_policy = self._traverse_policy(policy_payload=json.loads(policy.definition))
        _updated_object = self._deep_update(cluster.dict(exclude_none=True), traversed_policy)
        _updated_object = NewCluster(**_updated_object)
        _updated_object.policy_id = policy.policy_id
        return _updated_object

    @staticmethod
    def _get_policy(policy_service: PolicyService, policy_name: Optional[str], policy_id: Optional[str]) -> Policy:
        policy_name = policy_name if policy_name else policy_id.replace("cluster-policy://", "")
        all_policies = PoliciesResponse(**policy_service.list_policies())
        relevant_policy = list(filter(lambda p: p.name == policy_name, all_policies.policies))

        if relevant_policy:
            if len(relevant_policy) != 1:
                raise ValueError(
                    f"More than one cluster policy with name {policy_name} found."
                    f"Available policies are: {all_policies}"
                )
            return relevant_policy[0]

        raise ValueError(
            f"No cluster policies were fund under name {policy_name}."
            f"Available policy names are: {[p.name for p in all_policies.policies]}"
        )

    @staticmethod
    def _append_init_scripts(policy_init_scripts: List, existing_init_scripts: List) -> List:
        final_init_scripts = deepcopy(policy_init_scripts)
        flat_policy_init_scripts = defaultdict(list)
        for script in policy_init_scripts:
            for k, v in script.items():
                flat_policy_init_scripts[k].append(v["destination"])
        for script in existing_init_scripts:
            for k, v in script.items():
                if not v or not v.get("destination"):
                    raise Exception("init_scripts section format is incorrect in the deployment file")
                destination = v["destination"]
                if destination not in flat_policy_init_scripts.get(k, []):
                    # deduplication and ensure init scripts from policy to run firstly
                    final_init_scripts.append(script)
        return final_init_scripts

    @classmethod
    def _deep_update(cls, d: Dict, u: Mapping) -> Dict:
        for k, v in u.items():
            if isinstance(v, Mapping):
                d[k] = cls._deep_update(d.get(k, {}), v)
            else:
                # if the key is already provided in deployment configuration, we need to verify the value
                # if value exists, we verify that it's the same as in the policy
                if existing_value := d.get(k):
                    if k == "init_scripts":
                        d[k] = PolicyAdjuster._append_init_scripts(v, existing_value)
                        continue
                    if existing_value != v:
                        err_msg = (
                            f"For key {k} there is a value in the cluster definition: {existing_value} \n"
                            f"However this value is fixed in the policy and shall be equal to: {v}."
                        )
                        raise ValueError(err_msg)
                d[k] = v
        return d

    @staticmethod
    def _traverse_policy(policy_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Idea of this function is the following:
        1. Walk through all items in the source policy
        2. Take only fixed policies
        3. parse the key:
            3.0 if there are no dots, key is a simple string
            3.1 key might be either a composite one, with dots - then we split this key by dots into a tuple
            3.2 a specific case is with spark_conf (such keys might have multiple dots after the spark_conf
        4. definitions will be added into parsed_props variable
        5. Generate Jobs API compatible dictionary with fixed properties
        :return: dictionary in a Jobs API compatible format
        """

        parsed_props: List[Tuple[Union[List[str], str], Any]] = []
        for key, definition in policy_payload.items():
            if definition.get("type") == "fixed":
                # preprocess key
                # for spark_conf keys might contain multiple dots
                if key.startswith("spark_conf"):
                    _key = key.split(".", 1)
                elif "." in key:
                    _key = key.split(".")
                else:
                    _key = key
                _value = definition["value"]
                parsed_props.append((_key, _value))

        result = {}
        init_scripts = {}

        for key_candidate, value in parsed_props:
            if isinstance(key_candidate, str):
                result[key_candidate] = value
            else:
                if key_candidate[0] == "init_scripts":
                    idx = int(key_candidate[1])
                    payload = {key_candidate[2]: {key_candidate[3]: value}}
                    init_scripts[idx] = payload
                else:
                    d = {key_candidate[-1]: value}
                    for _k in key_candidate[1:-1]:
                        d[_k] = d

                    updatable = result.get(key_candidate[0], {})
                    updatable.update(d)

                    result[key_candidate[0]] = updatable

        init_scripts = [init_scripts[k] for k in sorted(init_scripts)]
        if init_scripts:
            result["init_scripts"] = init_scripts

        return result
