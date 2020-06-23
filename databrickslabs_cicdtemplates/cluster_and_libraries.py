import sys

from databrickslabs_cicdtemplates import deployment


def create_cluster_and_install_libs(dir_name, pipeline_name, env=None, existing_cluster_id=None, install_libraries=True):
    model_name, exp_path, cloud = deployment.read_config()

    job_spec = deployment.check_if_dir_is_pipeline_def(dir_name + '/' + pipeline_name, cloud, env)

    if not job_spec:
        print('Cannot find pipeline ', pipeline_name, ' in directory ', dir, ' for the cloud ', cloud)
        sys.exit(-100)

    apiClient = deployment.getDatabricksAPIClient()

    deployment.set_mlflow_experiment_path(exp_path)

    if not existing_cluster_id:
        cluster_id = deployment.create_cluster(apiClient, job_spec,
                                               cluster_name='Cluster for ' + dir_name + '/' + pipeline_name)
        print('Created cluster with ID ', cluster_id)
        print('Waiting for cluster to start....')
        res = deployment.wait_for_cluster_to_start(apiClient, cluster_id)
        if res not in ['RUNNING']:
            print('Error starting the cluster ', cluster_id)
            sys.exit(-100)
    else:
        cluster_id = existing_cluster_id

    if install_libraries:
        libraries = deployment.prepare_libraries()
        if job_spec.get('libraries'):
            libraries = libraries + job_spec['libraries']
        run_id, artifact_uri, model_version, libraries, _ = deployment.log_artifacts(model_name, libraries, False)
        print('Installing libraries...')
        deployment.install_libraries(apiClient, dir_name, pipeline_name, cloud, env, cluster_id, libraries,
                                     artifact_uri)

    return cluster_id
