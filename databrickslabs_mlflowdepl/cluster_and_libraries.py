import sys

from databrickslabs_mlflowdepl import deployment


def main(args):
    model_name, exp_path, cloud = deployment.read_config()

    job_spec = deployment.check_if_dir_is_pipeline_def(args.dir_name + '/' + args.pipeline_name, cloud)

    if not job_spec:
        print('Cannot find pipeline ', args.pipeline_name, ' in directory ', dir, ' for the cloud ', cloud)
        sys.exit(-100)

    apiClient = deployment.getDatabricksAPIClient()

    deployment.set_mlflow_experiment_path(exp_path)

    libraries = deployment.prepare_libraries()
    if job_spec.get('libraries'):
        libraries = libraries + job_spec['libraries']
    run_id, artifact_uri, model_version, libraries = deployment.log_artifacts(model_name, libraries, False)

    if args.new_cluster:
        cluster_id = deployment.create_cluster(apiClient, job_spec)
        print('Created cluster with ID ', cluster_id)
        print('Waiting for cluster to start....')
        res = deployment.wait_for_cluster_to_start(apiClient, cluster_id)
        if res not in ['RUNNING']:
            print('Error starting the cluster ', cluster_id)
            sys.exit(-100)
    else:
        cluster_id = args.cluster_id

    print('Installing libraries...')
    deployment.install_libraries(apiClient, args.dir_name, args.pipeline_name, cloud, cluster_id, libraries,
                                 artifact_uri)
