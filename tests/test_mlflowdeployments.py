
from databrickslabs_cicdtemplates import dev_cicd_pipeline
from databrickslabs_cicdtemplates import release_cicd_pipeline
from databrickslabs_cicdtemplates import cluster_and_libraries
from databrickslabs_cicdtemplates import deployment
import unittest

from deployment import check_if_dir_is_pipeline_def


class TestMlflowDeployments(unittest.TestCase):
    def test_dev_cicd_pipeline(self):
        dev_cicd_pipeline.main('dev-tests')

    def test_create_prod_jobs(self):
        release_cicd_pipeline.main('', 'dev-tests', False)

    def test_create_cluster_with_libs(self):
        cluster_and_libraries.create_cluster_and_install_libs('dev-tests', 'pipeline1')

    def test_install_libs(self):
        api_client = deployment.getDatabricksAPIClient()
        job_spec = check_if_dir_is_pipeline_def('dev-tests/pipeline1', 'aws', None)
        cluster_id = deployment.create_cluster(api_client, job_spec)
        deployment.wait_for_cluster_to_start(api_client, cluster_id)
        cluster_and_libraries.create_cluster_and_install_libs('dev-tests', 'pipeline1', existing_cluster_id=cluster_id)


if __name__ == '__main__':
    unittest.main()
