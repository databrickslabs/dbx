import json
import os
from pathlib import Path, PurePosixPath
import re
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple

import git
import jinja2
import mlflow
import mlflow.entities
import ruamel.yaml
from databricks_cli.configure.config import _get_api_client  # noqa
from databricks_cli.configure.provider import (
    ProfileConfigProvider,
    EnvironmentVariableConfigProvider,
    DatabricksConfig,
)
from databricks_cli.sdk import ClusterService
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.workspace.api import WorkspaceService
from setuptools import sandbox

from dbx.api.configure import ConfigurationManager, EnvironmentInfo
from dbx.constants import DATABRICKS_MLFLOW_URI
from dbx.utils import dbx_echo
from dbx.utils.json import JsonUtils


def parse_multiple(multiple_argument: List[str]) -> Dict[str, str]:
    tags_splitted = [t.split("=") for t in multiple_argument]
    tags_dict = {t[0]: t[1] for t in tags_splitted}
    return tags_dict


class AbstractDeploymentConfig(ABC):
    def __init__(self, path):
        self._path = path

    @abstractmethod
    def get_environment(self, environment: str) -> Any:
        pass

    @abstractmethod
    def get_all_environment_names(self) -> Any:
        pass

    def _get_file_extension(self):
        return self._path.split(".").pop()


class YamlDeploymentConfig(AbstractDeploymentConfig):
    # only for reading purposes.
    # if you need to round-trip see this: https://yaml.readthedocs.io/en/latest/overview.html

    # ENV variable pattern and tag
    PATTERN = re.compile(r".*?\$\{([^}{:]+)(:[^}^{]+)?\}.*?")  # noqa
    YAML_TAG = "!ENV"

    def resolve_env_vars(self, loader: ruamel.yaml.SafeLoader, node: ruamel.yaml.Node):
        value = loader.construct_scalar(node)
        match = self.PATTERN.findall(value)

        full_value = value
        if match:
            for var in match:
                env_var_name, default_val = var
                env_val = os.environ.get(env_var_name, "")

                if env_val == "" and default_val:
                    env_val = default_val[1:]  # Remove the leading colon

                val_to_replace = "".join(var)
                full_value = full_value.replace(f"${{{val_to_replace}}}", env_val)

        return full_value

    def _read_yaml(self, file_path: str) -> Dict[str, Any]:
        loader = ruamel.yaml.SafeLoader

        # Tags indicate where to search for the pattern
        # In this case, it is !ENV
        loader.add_implicit_resolver(self.YAML_TAG, self.PATTERN, None)

        # Env variable resolver
        loader.add_constructor(self.YAML_TAG, self.resolve_env_vars)

        with open(file_path, "r", encoding="utf-8") as f:
            return ruamel.yaml.load(f, Loader=loader)

    def get_environment(self, environment: str) -> Any:
        return self._read_yaml(self._path).get("environments").get(environment)

    def get_all_environment_names(self) -> List[str]:
        return list(self._read_yaml(self._path).get("environments").keys())


class JsonDeploymentConfig(AbstractDeploymentConfig):
    # ENV variable pattern
    PATTERN = re.compile(r"\$\{([^}{:]+)(:[^}^{]+)?\}")  # noqa

    def resolve_env_vars(self, json_obj: Dict[str, Any]) -> Dict[str, Any]:
        json_str = json.dumps(json_obj)

        def _env_resolver(match):
            env_var_name, default_val = match.group(1, 2)
            env_val = os.environ.get(env_var_name, "")

            if env_val == "" and default_val:
                env_val = default_val[1:]  # Remove the leading colon

            return json.dumps(env_val)[1:-1]

        return json.loads(self.PATTERN.sub(_env_resolver, json_str))

    def get_environment(self, environment: str) -> Any:
        return self.resolve_env_vars(JsonUtils.read(Path(self._path))).get(environment)

    def get_all_environment_names(self) -> Any:
        return list(self.resolve_env_vars(JsonUtils.read(Path(self._path))).keys())


class Jinja2DeploymentConfig(AbstractDeploymentConfig):
    def __init__(self, path, ext):
        super().__init__(path)
        self._ext = ext

    def _render_jinja_template(self) -> str:
        root_dir = "."
        # Jinja processes templates by path relative to provided FileSystemLoader
        # Jinja expects paths always with forward separators, because the "paths" in
        # jinja are not actual filesystem paths, but often have a one-to-one mapping.
        # To retain platform independence, we replace whatever the operating system's
        # preferred separator is with a forward slash to keep compatibility with jinja.
        template_path = os.path.relpath(self._path, start=root_dir).replace(os.sep, "/")
        # We render templates with an environment from the root directory so
        # that other template includes can be processed
        j2_env = jinja2.Environment(loader=jinja2.FileSystemLoader(root_dir))
        return j2_env.get_template(template_path).render(os.environ)

    def _get_deployment_config(self) -> Dict[str, Any]:
        template = self._render_jinja_template()
        if self._ext == "json":
            return json.loads(template)
        elif self._ext in ["yml", "yaml"]:
            yaml = ruamel.yaml.YAML(typ="safe")
            return yaml.load(template).get("environments")

    def get_environment(self, environment: str) -> Any:
        return self._get_deployment_config().get(environment)

    def get_all_environment_names(self) -> Any:
        return list(self._get_deployment_config().keys())


def get_deployment_config(path: str) -> AbstractDeploymentConfig:
    ext = path.split(".").pop()
    if ext == "json":
        return JsonDeploymentConfig(path)
    elif ext in ["yml", "yaml"]:
        return YamlDeploymentConfig(path)
    elif ext == "j2":
        second_ext = path.split(".")[-2]
        return Jinja2DeploymentConfig(path, second_ext)
    else:
        raise Exception(f"Undefined config file handler for extension: {ext}")


def generate_filter_string(env: str) -> str:
    general_filters = [
        f"tags.dbx_environment = '{env}'",
        "tags.dbx_status = 'SUCCESS'",
        "tags.dbx_action_type = 'deploy'",
    ]

    branch_name = get_current_branch_name()
    if branch_name:
        general_filters.append(f"tags.dbx_branch_name = '{branch_name}'")

    all_filters = general_filters
    filter_string = " and ".join(all_filters)
    return filter_string


def _prepare_workspace_dir(api_client: ApiClient, ws_dir: str):
    p = str(PurePosixPath(ws_dir).parent)
    service = WorkspaceService(api_client)
    service.mkdirs(p)


def get_environment_data(environment: str) -> EnvironmentInfo:
    environment_data = ConfigurationManager().get(environment)

    if not environment_data:
        raise Exception(f"No environment {environment} provided in the project file")
    return environment_data


def pick_config(profile: str) -> Tuple[str, DatabricksConfig]:
    config = EnvironmentVariableConfigProvider().get_config()
    if config:
        config_type = "ENV"
        dbx_echo("Using configuration from the environment variables")
    else:
        dbx_echo("No environment variables provided, using the ~/.databrickscfg")
        config = ProfileConfigProvider(profile).get_config()
        config_type = "PROFILE"
        if not config:
            raise Exception(f"""Couldn't get profile with name: {profile}. Please check the config settings""")
    return config_type, config


def prepare_environment(environment: str) -> ApiClient:
    environment_data = get_environment_data(environment)

    config_type, config = pick_config(environment_data.profile)

    try:
        api_client = _get_api_client(config, command_name="cicdtemplates-")
    except IndexError as e:
        dbx_echo(
            """
        Error during initializing the API client.
        Probably, env variable DATABRICKS_HOST is set, but cannot be properly parsed.
        Please verify that DATABRICKS_HOST is in format https://<your-workspace-host>
        Original exception is below:
        """
        )
        raise e

    _prepare_workspace_dir(api_client, environment_data.workspace_dir)

    if config_type == "ENV":
        mlflow.set_tracking_uri(DATABRICKS_MLFLOW_URI)
    elif config_type == "PROFILE":
        mlflow.set_tracking_uri(f"{DATABRICKS_MLFLOW_URI}://{environment_data.profile}")
    else:
        raise NotImplementedError(f"Config type: {config_type} is not implemented")

    experiment: Optional[mlflow.entities.Experiment] = mlflow.get_experiment_by_name(environment_data.workspace_dir)

    # if there is no experiment
    if not experiment:
        mlflow.create_experiment(environment_data.workspace_dir, environment_data.artifact_location)
    else:
        # verify experiment location
        if experiment.artifact_location != environment_data.artifact_location:
            raise Exception(
                f"Required location of experiment {environment_data.workspace_dir} "
                f"doesn't match the project defined one: \n"
                f"\t experiment artifact location: {experiment.artifact_location} \n"
                f"\t project artifact location   : {environment_data.artifact_location} \n"
                f"Change of experiment location is currently not supported in MLflow. "
                f"Please change the experiment name to create a new experiment."
            )

    mlflow.set_experiment(environment_data.workspace_dir)

    return api_client


def get_package_file() -> Optional[Path]:
    dbx_echo("Locating package file")
    file_locator = list(Path("dist").glob("*.whl"))
    sorted_locator = sorted(file_locator, key=os.path.getmtime)  # get latest modified file, aka latest package version
    if sorted_locator:
        file_path = sorted_locator[-1]
        dbx_echo(f"Package file located in: {file_path}")
        return file_path
    else:
        dbx_echo("Package file was not found")
        return None


def handle_package(rebuild_arg):
    if rebuild_arg:
        dbx_echo("No rebuild will be done, please ensure that the package distribution is in dist folder")
    else:
        dbx_echo("Re-building package")
        if not Path("setup.py").exists():
            raise Exception(
                "No setup.py provided in project directory. Please create one, or disable rebuild via --no-rebuild"
            )
        sandbox.run_setup("setup.py", ["-q", "clean", "bdist_wheel"])
        dbx_echo("Package re-build finished")


def get_current_branch_name() -> Optional[str]:
    if "GITHUB_REF" in os.environ:
        ref = os.environ["GITHUB_REF"].split("/")
        return ref[-1]
    else:
        try:
            repo = git.Repo(".", search_parent_directories=True)
            if repo.head.is_detached:
                return None
            else:
                return repo.active_branch.name
        except git.InvalidGitRepositoryError:
            return None


def _preprocess_cluster_args(api_client: ApiClient, cluster_name: Optional[str], cluster_id: Optional[str]) -> str:
    cluster_service = ClusterService(api_client)

    if not cluster_name and not cluster_id:
        raise RuntimeError("Parameters --cluster-name and --cluster-id couldn't be empty at the same time.")

    if cluster_name:

        existing_clusters = [
            c for c in cluster_service.list_clusters().get("clusters") if not c.get("cluster_name").startswith("job-")
        ]
        matching_clusters = [c for c in existing_clusters if c.get("cluster_name") == cluster_name]

        if not matching_clusters:
            cluster_names = [c["cluster_name"] for c in existing_clusters]
            raise NameError(f"No clusters with name {cluster_name} found. Available clusters are: {cluster_names} ")
        if len(matching_clusters) > 1:
            raise NameError(f"Found more then one cluster with name {cluster_name}: {matching_clusters}")

        cluster_id = matching_clusters[0]["cluster_id"]
    else:
        if not cluster_service.get_cluster(cluster_id):
            raise NameError(f"Cluster with id {cluster_id} not found")

    return cluster_id
