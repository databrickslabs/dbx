import os
from pathlib import PurePosixPath
from typing import Optional
from urllib.parse import urlparse

import mlflow
from databricks_cli.sdk import WorkspaceService
from mlflow.entities import Experiment

from dbx.api.auth import AuthConfigProvider
from dbx.api.clients.databricks_api import DatabricksClientProvider
from dbx.constants import DATABRICKS_MLFLOW_URI
from dbx.models.project import MlflowArtifactStorageProperties


class MlflowStorageConfigurationManager:
    DATABRICKS_HOST_ENV: str = "DATABRICKS_HOST"
    DATABRICKS_TOKEN_ENV: str = "DATABRICKS_TOKEN"

    @staticmethod
    def _strip_url(url: str) -> str:
        """
        Mlflow API requires url to be stripped, e.g.
        {scheme}://{netloc}/some-stuff/ shall be transformed to {scheme}://{netloc}
        :param url: url to be stripped
        :return: stripped url
        """
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _setup_tracking_uri(self):
        os.environ[self.DATABRICKS_HOST_ENV] = self._strip_url(self._config.host)
        os.environ[self.DATABRICKS_TOKEN_ENV] = self._config.token
        mlflow.set_tracking_uri(DATABRICKS_MLFLOW_URI)

    def __init__(self, properties: MlflowArtifactStorageProperties):
        self._config = AuthConfigProvider().get_config()
        self._properties = properties

    def prepare(self):
        self._setup_tracking_uri()
        self._prepare_workspace_dir()
        self._setup_experiment()

    def _prepare_workspace_dir(self):
        api_client = DatabricksClientProvider().get_v2_client()
        p = str(PurePosixPath(self._properties.workspace_directory).parent)
        service = WorkspaceService(api_client)
        service.mkdirs(p)

    def _setup_experiment(self):
        experiment: Optional[Experiment] = mlflow.get_experiment_by_name(self._properties.workspace_directory)

        if not experiment:
            mlflow.create_experiment(self._properties.workspace_directory, self._properties.artifact_location)
        else:
            # verify experiment location
            if experiment.artifact_location != self._properties.artifact_location:
                raise Exception(
                    f"Required location of experiment {self._properties.workspace_directory} "
                    f"doesn't match the project defined one: \n"
                    f"\t experiment artifact location: {experiment.artifact_location} \n"
                    f"\t project artifact location   : {self._properties.artifact_location} \n"
                    f"Change of experiment location is currently not supported in Mlflow. "
                    f"Please change the experiment name (workspace_directory property) to create a new experiment."
                )

        mlflow.set_experiment(self._properties.workspace_directory)
