import os
from pathlib import PurePosixPath
from typing import Optional
from urllib.parse import urlparse

import mlflow
from databricks_cli.sdk import WorkspaceService
from mlflow.entities import Experiment

from dbx.api.auth import AuthConfigProvider
from dbx.api.client_provider import DatabricksClientProvider
from dbx.constants import DATABRICKS_MLFLOW_URI
from dbx.api.configure import EnvironmentInfo


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

    @classmethod
    def _setup_tracking_uri(cls):
        config = AuthConfigProvider.get_config()
        os.environ[cls.DATABRICKS_HOST_ENV] = cls._strip_url(config.host)
        os.environ[cls.DATABRICKS_TOKEN_ENV] = config.token
        mlflow.set_tracking_uri(DATABRICKS_MLFLOW_URI)

    @classmethod
    def prepare(cls, properties: EnvironmentInfo):
        cls._setup_tracking_uri()
        cls._prepare_workspace_dir(properties)
        cls._setup_experiment(properties)

    @classmethod
    def _prepare_workspace_dir(cls, properties: EnvironmentInfo):
        api_client = DatabricksClientProvider().get_v2_client()
        p = str(PurePosixPath(properties.workspace_dir).parent)
        service = WorkspaceService(api_client)
        service.mkdirs(p)

    @classmethod
    def _setup_experiment(cls, properties: EnvironmentInfo):
        experiment: Optional[Experiment] = mlflow.get_experiment_by_name(properties.workspace_dir)

        if not experiment:
            mlflow.create_experiment(properties.workspace_dir, properties.artifact_location)
        else:
            # verify experiment location
            if experiment.artifact_location != properties.artifact_location:
                raise Exception(
                    f"Required location of experiment {properties.workspace_dir} "
                    f"doesn't match the project defined one: \n"
                    f"\t experiment artifact location: {experiment.artifact_location} \n"
                    f"\t project artifact location   : {properties.artifact_location} \n"
                    f"Change of experiment location is currently not supported in Mlflow. "
                    f"Please change the experiment name (workspace_directory property) to create a new experiment."
                )

        mlflow.set_experiment(properties.workspace_dir)
