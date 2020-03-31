import sys
from databrickslabs_mlflowdepl import deployment
import mlflow


def main(dir):
    apiClient = deployment.getDatabricksAPIClient()

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

    res = deployment.submit_jobs(apiClient, dir, artifact_uri, libraries, cloud)
    if not res:
        print('Tests were not successful. Quitting..')
        sys.exit(-100)


if __name__ == "__main__":
    main('dev-tests')
