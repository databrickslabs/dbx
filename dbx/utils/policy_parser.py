"""
This policy parser is based on:
- API Doc: policy parser is based on API doc https://docs.databricks.com/dev-tools/api/latest/policies.html
- Policy definition docs:
    - AWS: https://docs.databricks.com/administration-guide/clusters/policies.html#cluster-policy-attribute-paths
    - Azure: https://docs.microsoft.com/en-us/azure/databricks/administration-guide/clusters/policies
    - GCP: Cluster policies were not supported at the moment of 0.1.3 release.
Please note that only "fixed" values will be automatically added to the job definition.
"""
from typing import Dict, Any, Tuple, List, Union


class PolicyParser:
    def __init__(self, policy: Dict[str, Dict[str, Any]]):
        self.source_policy = policy

    def parse(self) -> Dict[str, Any]:
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
        for key, definition in self.source_policy.items():
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
