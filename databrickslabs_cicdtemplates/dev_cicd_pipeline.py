import sys
import os
from databrickslabs_cicdtemplates import deployment


def main(dir, name=None, env=None):
    apiClient = deployment.getDatabricksAPIClient()

    model_name, exp_path, cloud = deployment.read_config()

    deployment.set_mlflow_experiment_path(exp_path)

    libraries = deployment.prepare_libraries()
    run_id, artifact_uri, model_version, libraries, _ = deployment.log_artifacts(model_name, libraries, False)

    res = deployment.submit_jobs_for_all_pipelines(apiClient, dir, artifact_uri, libraries, cloud, env, pipeline_name=name)
    if not res:
        print('Tests were not successful. Quitting..')
        sys.exit(-100)

if __name__ == "__main__":
    main('dev-tests')
