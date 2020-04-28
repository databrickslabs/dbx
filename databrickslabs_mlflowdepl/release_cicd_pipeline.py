import sys
from os import path
import mlflow

from databrickslabs_mlflowdepl import deployment


def main(test_folder, prod_folder, do_test):
    apiClient = deployment.getDatabricksAPIClient()

    model_name, exp_path, cloud = deployment.read_config()

    deployment.set_mlflow_experiment_path(exp_path)

    libraries = deployment.prepare_libraries()
    run_id, artifact_uri, model_version, libraries, _ = deployment.log_artifacts(model_name, libraries, True)

    if do_test:
        res = deployment.submit_jobs_for_all_pipelines(apiClient, test_folder, artifact_uri, libraries, cloud)
        if not res:
            print('Tests were not successful. Quitting..')
            sys.exit(-100)

    if path.exists(prod_folder):
        deployment.create_or_update_production_jobs(apiClient, prod_folder, run_id, artifact_uri, libraries,
                                                    cloud, model_name, ["production"], model_version)


if __name__ == "__main__":
    main('integration-tests', 'pipelines', True)
