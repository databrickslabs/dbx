import os
from pathlib import PurePosixPath
from typing import Optional

import mlflow
from databricks_cli.sdk import WorkspaceService
from mlflow.entities import Experiment
from mlflow.exceptions import RestException

from dbx.api.auth import AuthConfigProvider
from dbx.api.client_provider import DatabricksClientProvider
from dbx.api.configure import EnvironmentInfo
from dbx.constants import DATABRICKS_MLFLOW_URI
from dbx.utils.url import strip_databricks_url


class MlflowStorageConfigurationManager:
    DATABRICKS_HOST_ENV: str = "DATABRICKS_HOST"
    DATABRICKS_TOKEN_ENV: str = "DATABRICKS_TOKEN"

    @classmethod
    def _setup_tracking_uri(cls):
        config = AuthConfigProvider.get_config()
        os.environ[cls.DATABRICKS_HOST_ENV] = strip_databricks_url(config.host)
        os.environ[cls.DATABRICKS_TOKEN_ENV] = config.token
        mlflow.set_tracking_uri(DATABRICKS_MLFLOW_URI)

    @classmethod
    def prepare(cls, properties: EnvironmentInfo):
        cls._setup_tracking_uri()
        cls._prepare_workspace_dir(properties)
        cls._setup_experiment(properties)

    @classmethod
    def _prepare_workspace_dir(cls, env: EnvironmentInfo):
        api_client = DatabricksClientProvider().get_v2_client()
        p = str(PurePosixPath(env.properties.workspace_directory).parent)
        service = WorkspaceService(api_client)
        service.mkdirs(p)

    @staticmethod
    def _get_experiment_safe(name: str) -> Optional[Experiment]:
        try:
            experiment = mlflow.get_experiment_by_name(name)
            return experiment
        except RestException as e:
            text_check = "does not exist." in str(e)
            error_check = "INVALID_PARAMETER_VALUE" in str(e)
            if text_check and error_check:
                return None
            else:
                raise e

    @classmethod
    def _setup_experiment(cls, env: EnvironmentInfo):
        experiment: Optional[Experiment] = cls._get_experiment_safe(env.properties.workspace_directory)

        if not experiment:
            mlflow.create_experiment(env.properties.workspace_directory, env.properties.artifact_location)
        else:
            # verify experiment location
            if experiment.artifact_location != env.properties.artifact_location:
                raise Exception(
                    f"Required location of experiment {env.properties.workspace_directory} "
                    f"doesn't match the project defined one: \n"
                    f"\t experiment artifact location: {experiment.artifact_location} \n"
                    f"\t project artifact location   : {env.properties.artifact_location} \n"
                    f"Change of experiment location is currently not supported in Mlflow. "
                    f"Please change the experiment name (workspace_directory property) to create a new experiment."
                )

        mlflow.set_experiment(env.properties.workspace_directory)
