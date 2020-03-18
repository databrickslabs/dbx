import sys
from os import path

import mlflow
import pkg_resources

from mlflowdepl import deployment


def main(test_folder, prod_folder, do_test):
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
    from databricks_cli.configure.provider import get_config
    from databricks_cli.configure.config import _get_api_client

    apiClient = _get_api_client(get_config(), command_name='mlflow_deployments-' + version)

    if do_test:
        res = deployment.submit_jobs(apiClient, test_folder, run_id, artifact_uri, libraries, cloud, version)
        if not res:
            print('Tests were not successful. Quitting..')
            sys.exit(-100)

    if path.exists(prod_folder):
        deployment.create_or_update_production_jobs(apiClient, prod_folder, run_id, artifact_uri, libraries,
                                                    cloud, version, model_name, ["production"], model_version)


if __name__ == "__main__":
    main('integration-tests', 'pipelines', True)
