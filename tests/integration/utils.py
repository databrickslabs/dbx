import json

from cookiecutter.main import cookiecutter
from databricks_cli.sdk.api_client import ApiClient
from retry import retry

CICD_TEMPLATES_URI = "https://github.com/databrickslabs/cicd-templates.git"


@retry(tries=10, delay=5, backoff=5)
def initialize_cookiecutter(project_name):
    cookiecutter(CICD_TEMPLATES_URI, no_input=True, extra_context={"project_name": project_name})


@retry(tries=10, delay=5, backoff=5)
def permanent_delete_cluster(api_client: ApiClient, cluster_id: str):
    payload = {"cluster_id": cluster_id}
    full_uri = api_client.url + "/clusters/permanent-delete"
    api_client.session.request("POST", full_uri, data=json.dumps(payload),
                               verify=True,
                               headers=api_client.default_headers, timeout=30)
