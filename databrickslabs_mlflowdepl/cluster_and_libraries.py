import string
import sys
from os import path
import mlflow

from databrickslabs_mlflowdepl import deployment


def main(args):
    if args.cluster_id and args.new_cluster:
        print('create_cluster parameter is set to True and cluster_id is specified. Exiting...')
        sys.exit(-100)
    if not (args.cluster_id) and not (args.new_cluster):
        print('create_cluster parameter is set to False and cluster_id is not specified. Exiting...')
        sys.exit(-100)

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
    run_id, artifact_uri, model_version, libraries = deployment.log_artifacts(model_name, libraries)

    if args.new_cluster:
        cluster_id = deployment.create_cluster(apiClient, args.dir_name, args.pipeline_name, cloud)
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
