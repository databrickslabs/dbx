import pathlib
from typing import Optional, List

from dbx.api.config_reader import ConfigReader
from dbx.models.deployment import Environment, WorkloadDefinition
from dbx.utils import dbx_echo


class DeploymentArgumentsPreprocessor:
    @staticmethod
    def preprocess_deployment_file(file_path: Optional[pathlib.Path]) -> pathlib.Path:
        if file_path:
            if file_path.exists():
                dbx_echo(f"Using the provided deployment file {file_path}")
                return file_path
            else:
                raise Exception(f"Provided deployment file {file_path} is non-existent.")
        else:
            potential_extensions = ["json", "yml", "yaml", "json.j2", "yaml.j2", "yml.j2"]

            for ext in potential_extensions:
                candidate = pathlib.Path(f"conf/deployment.{ext}")
                if candidate.exists():
                    dbx_echo(f"Auto-discovery found deployment file {candidate}")
                    return candidate

            raise Exception(
                "Auto-discovery was unable to find any suitable deployment file in the conf directory. "
                "Please provide file name via --deployment-file option"
            )

    @staticmethod
    def preprocess_config(deployment_file: pathlib.Path, environment_name: str) -> Environment:
        config_reader = ConfigReader(deployment_file)
        environment = config_reader.get_environment(environment_name)
        if not environment:
            raise Exception(
                f"""
            Requested environment {environment} is non-existent in the deployment file {deployment_file}.
            Available environments are: {config_reader.get_all_environment_names()}
            """
            )
        if len(environment.workloads) == 0:
            raise Exception(f"No workloads provided in the environment {environment_name}.")
        return environment

    @staticmethod
    def preprocess_workload(env: Environment, workload: str, all_workloads: bool) -> List[WorkloadDefinition]:
        if all_workloads:
            return env.workloads
        elif workload in [w.name for w in env.workloads]:
            return [w for w in env.workloads if w.name == workload]
        else:
            raise NameError(
                f"Workload {workload} was not found in environment. Available workload names are {env.workloads}"
            )
