import string
import sys
from os import path
import mlflow

from databrickslabs_mlflowdepl import deployment
from deployment import check_if_dir_is_pipeline_def


def main(args):
    if args.cluster_id and args.new_cluster:
        print('create_cluster parameter is set to True and cluster_id is specified. Exiting...')
        sys.exit(-100)
    if not (args.cluster_id) and not (args.new_cluster):
        print('create_cluster parameter is set to False and cluster_id is not specified. Exiting...')
        sys.exit(-100)

    model_name, exp_path, cloud = deployment.read_config()

    job_spec = check_if_dir_is_pipeline_def(args.dir_name + '/' + args.pipeline_name, cloud)

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
        res = deployment.wait_for_cluster_to_start(apiClient, cluster_id)
        if res not in ['RUNNING']:
            print('Error starting the cluster ', cluster_id)
            sys.exit(-100)
    else:
        cluster_id = args.cluster_id

    deployment.install_libraries(apiClient, args.dir_name, args.pipeline_name, cloud, cluster_id, libraries,
                                 artifact_uri)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-name", help="Pipeline Name")
    parser.add_argument("--dir-name", help="Directory")
    parser.add_argument("--cluster-id", help="Cluster ID")
    parser.add_argument("--new-cluster", help="Create new cluster", action="store_true")
    args = parser.parse_args()
    print(args)
    main(args)
