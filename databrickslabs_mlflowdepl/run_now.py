import sys
from databrickslabs_mlflowdepl import deployment
from databrickslabs_mlflowdepl import excontextmgmt


def main(pipeline_dir, pipeline_name, cluster_id, reuse_ctx=True):
    apiClient = deployment.getDatabricksAPIClient()

    model_name, exp_path, cloud = deployment.read_config()

    deployment.set_mlflow_experiment_path(exp_path)

    execution_context_id = None
    libraries = None
    if reuse_ctx:
        print('Reading execution context...')
        execution_context_id = excontextmgmt.get_existing_execution_context_id(cluster_id)
        print('Found existing execution context with ID: ', execution_context_id)

    if (not reuse_ctx) or (execution_context_id is None):
        print('Creating new execution context...')
        execution_context_id = deployment.create_exution_context_exists(apiClient,cluster_id)
        print('Created execution context with ID: ', execution_context_id)
        excontextmgmt.store_execution_context_id(cluster_id, execution_context_id)

        libraries = deployment.prepare_libraries()

    run_id, artifact_uri, model_version, libraries, current_artifacts = deployment.log_artifacts(model_name, libraries,
                                                                              register_model=False,
                                                                              dirs_to_deploy=['dependencies',
                                                                                              pipeline_dir])

    deployment.submit_one_pipeline_to_exctx(apiClient, pipeline_dir, pipeline_name, libraries, current_artifacts, cloud,
                                            cluster_id, execution_context_id)
