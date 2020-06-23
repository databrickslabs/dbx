import os
import sys
from databrickslabs_cicdtemplates import deployment
from databrickslabs_cicdtemplates import cluster_and_libraries
from databrickslabs_cicdtemplates.configmgmt import Config

from databricks_cli.sdk import ClusterService


def ensure_cluster_is_running(apiClient, clusterSrv, cluster_id):
    cluster_spec = clusterSrv.get_cluster(cluster_id)
    if cluster_spec['state'] in ['TERMINATED', 'TERMINATING']:
        clusterSrv.start_cluster(cluster_id)
        deployment.wait_for_cluster_to_start(apiClient, cluster_id)


def main(pipeline_dir, pipeline_name, env=None, cluster_id=None, reuse_ctx=True, any_cluster=False):
    apiClient = deployment.getDatabricksAPIClient()
    clusterSrv = ClusterService(apiClient)

    model_name, exp_path, cloud = deployment.read_config()

    conf = Config()

    deployment.set_mlflow_experiment_path(exp_path)

    execution_context_id = None
    libraries = None
    dirs_to_deploy = [pipeline_dir]

    try:
        if cluster_id is None:
            cluster_id = conf.get_cluster_id(pipeline_dir, pipeline_name, any=any_cluster)
        if cluster_id is not None:
            print('Found cluster ID ',cluster_id)
            print('Ensuring that cluster is running...')
            ensure_cluster_is_running(apiClient, clusterSrv, cluster_id)
    except Exception as e:
        print('Error has occured while starting/creating the cluster: ', e)
        cluster_id = None

    if cluster_id is None:
        print('Creating new cluster...')
        cluster_id = cluster_and_libraries.create_cluster_and_install_libs(pipeline_dir, pipeline_name,
                                                                           install_libraries=False)
        print('Created new cluster with ID: ', cluster_id)
        conf.store_cluster_for_pipeline(pipeline_dir, pipeline_name, cluster_id)

    if reuse_ctx:
        print('Reading execution context...')
        execution_context_id = conf.get_existing_execution_context_id(cluster_id)
        print('Found existing execution context with ID: ', execution_context_id)
        if not deployment.ensure_exution_context_exists(apiClient, cluster_id, execution_context_id):
            execution_context_id = None

    if (not reuse_ctx) or (execution_context_id is None):
        print('Creating new execution context...')
        execution_context_id = deployment.create_exution_context_exists(apiClient, cluster_id)
        if execution_context_id is None:
            print('Failed creating execution context. Exiting..')
            sys.exit(-100)
        print('Created execution context with ID: ', execution_context_id)
        conf.store_execution_context_id(cluster_id, execution_context_id)

        libraries = deployment.prepare_libraries()
        dirs_to_deploy.append('dependencies')

    run_id, artifact_uri, model_version, libraries, current_artifacts = deployment.log_artifacts(model_name, libraries,
                                                                                                 register_model=False,
                                                                                                 dirs_to_deploy=dirs_to_deploy)

    deployment.submit_one_pipeline_to_exctx(apiClient, artifact_uri, pipeline_dir, pipeline_name, libraries,
                                            current_artifacts, cloud,env,
                                            cluster_id, execution_context_id)
