from pathlib import Path
from typing import List

from httpx import Client, BasicAuth, codes

from dbx.api.auth import AuthConfigProvider
from dbx.api.storage.mlflow_based import MlflowStorageConfigurationManager
from dbx.driver_server.models import ServerInfo


class FileServerClient:
    def __init__(self, workspace_id: str, cluster_id: str, port: int = 6006):
        self._config = AuthConfigProvider.get_config()
        self._url = self.build_uri(
            host=MlflowStorageConfigurationManager.strip_url(self._config.host),
            workspace_id=workspace_id,
            cluster_id=cluster_id,
            port=port,
        )
        self._client = Client(auth=BasicAuth("token", self._config.token), base_url=self._url)

    @staticmethod
    def build_uri(host: str, workspace_id: str, cluster_id: str, port: int):
        return f"{host}/driver-proxy-api/o/{workspace_id}/{cluster_id}/{port}"

    def get_server_info(self) -> ServerInfo:
        response = self._client.get("/info")
        if not response.status_code == codes.OK:
            raise Exception(
                "dbx server is not launched on the driver machine."
                "Please start the server accordingly to the dbx docs."
            )
        return ServerInfo(**response.json())

    def upload_files(self, context_id: str, files: List[Path]):
        method_path = "files"
        file_list = [(method_path, (p.name, p.read_bytes(), "application/octet-stream")) for p in files]
        response = self._client.post(f"/upload/{context_id}", files=file_list)
        if response.status_code != codes.OK:
            raise Exception(f"Unexpected server status code: {response.status_code} with text: {response.text}")
