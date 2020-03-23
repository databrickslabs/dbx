import sys
import pkg_resources

from databrickslabs_mlflowdepl import deployment
import mlflow

# run test job
from databricks_cli.configure.provider import get_config
from databricks_cli.configure.config import _get_api_client


def main(dir):

    version = pkg_resources.get_distribution("mlflowdepl").version

    model_name, exp_path, cloud = deployment.read_config()

    try:
        mlflow.set_experiment(exp_path)
    except Exception as e:
        raise Exception(f"""{e}.
        Have you added the following secrets to your github repo?
            secrets.DATABRICKS_HOST
            secrets.DATABRICKS_TOKEN""")

    libraries = deployment.prepare_libraries()
    run_id, artifact_uri, model_version = deployment.log_artifacts(model_name, libraries)

    # run test job
    apiClient = _get_api_client(get_config(), command_name='mlflow_deployments-'+version)

    res = deployment.submit_jobs(apiClient, dir, run_id, artifact_uri, libraries, cloud, version)
    if not res:
        print('Tests were not successful. Quitting..')
        sys.exit(-100)

if __name__ == "__main__":
    main('dev-tests')