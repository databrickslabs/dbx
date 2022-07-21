import copy
from typing import Dict, Any

import requests
from databricks_cli.sdk import ApiClient
from retry import retry

from dbx.api.auth import AuthConfigProvider


class ApiV1Client:
    def __init__(self, api_client: ApiClient):
        self.v1_client = copy.deepcopy(api_client)
        self.v1_client.api_version = "1.2"

    def get_command_status(self, payload) -> Dict[Any, Any]:
        result = self.v1_client.perform_query(method="GET", path="/commands/status", data=payload)
        return result

    def cancel_command(self, payload) -> None:
        self.v1_client.perform_query(method="POST", path="/commands/cancel", data=payload)

    def execute_command(self, payload) -> Dict[Any, Any]:
        result = self.v1_client.perform_query(method="POST", path="/commands/execute", data=payload)
        return result

    def get_context_status(self, payload):
        try:
            result = self.v1_client.perform_query(method="GET", path="/contexts/status", data=payload)
            return result
        except requests.exceptions.HTTPError:
            return None

    # sometimes cluster is already in the status="RUNNING", however it couldn't yet provide execution context
    # to make the execute command stable is such situations, we add retry handler.
    @retry(tries=10, delay=5, backoff=5)
    def create_context(self, payload):
        result = self.v1_client.perform_query(method="POST", path="/contexts/create", data=payload)
        return result


class DatabricksClientProvider:
    """
    Provides both v1/v2 clients for Databricks API.
    """

    @classmethod
    def _get_v2_client(cls) -> ApiClient:
        config = AuthConfigProvider.get_config()
        verify = config.insecure is None
        _client = ApiClient(
            host=config.host,
            token=config.token,
            verify=verify,
            command_name="cicdtemplates-",
        )
        return _client

    @classmethod
    def _get_v1_client(cls) -> ApiV1Client:
        _client = ApiV1Client(cls._get_v2_client())
        return _client

    @classmethod
    def get_v2_client(cls) -> ApiClient:
        return cls._get_v2_client()

    @classmethod
    def get_v1_client(cls) -> ApiV1Client:
        return cls._get_v1_client()
