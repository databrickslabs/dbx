import sys
from databrickslabs_mlflowdepl import deployment
from databrickslabs_mlflowdepl import excontextmgmt


def main(pipeline_dir, pipeline_name, cluster_id, reuse_ctx=True):
    apiClient = deployment.getDatabricksAPIClient()

    model_name, exp_path, cloud = deployment.read_config()

    deployment.set_mlflow_experiment_path(exp_path)

    execution_context_id = None
    libraries = None
    dirs_to_deploy = [pipeline_dir]
    if reuse_ctx:
        print('Reading execution context...')
        execution_context_id = excontextmgmt.get_existing_execution_context_id(cluster_id)
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
        excontextmgmt.store_execution_context_id(cluster_id, execution_context_id)

        libraries = deployment.prepare_libraries()
        dirs_to_deploy.append('dependencies')

    run_id, artifact_uri, model_version, libraries, current_artifacts = deployment.log_artifacts(model_name, libraries,
                                                                                                 register_model=False,
                                                                                                 dirs_to_deploy=dirs_to_deploy)

    deployment.submit_one_pipeline_to_exctx(apiClient, artifact_uri, pipeline_dir, pipeline_name, libraries,
                                            current_artifacts, cloud,
                                            cluster_id, execution_context_id)
