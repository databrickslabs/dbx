from collections import namedtuple

from databrickslabs_mlflowdepl import dev_cicd_pipeline
from databrickslabs_mlflowdepl import release_cicd_pipeline
from databrickslabs_mlflowdepl import cluster_and_libraries
from databrickslabs_mlflowdepl import deployment
import unittest

class TestMlflowDeployments(unittest.TestCase):
    def test_dev_cicd_pipeline(self):
        dev_cicd_pipeline.main('dev-tests')

    def test_create_prod_jobs(self):
        release_cicd_pipeline.main('','dev-tests',False)

    def test_create_cluster_with_libs(self):
        Arg = namedtuple('Arg', ['cluster_id', 'dir_name', 'new_cluster', 'pipeline_name'])
        cluster_and_libraries.main(Arg(cluster_id=None, dir_name='dev-tests', new_cluster=True, pipeline_name='pipeline1'))

    def test_install_libs(self):
        Arg = namedtuple('Arg', ['cluster_id', 'dir_name', 'new_cluster', 'pipeline_name'])
        api_client = deployment.getDatabricksAPIClient()
        cluster_id = deployment.create_cluster(api_client, 'dev-tests', 'pipeline1', 'aws')
        deployment.wait_for_cluster_to_start(api_client, cluster_id)
        cluster_and_libraries.main(Arg(cluster_id=cluster_id, dir_name='dev-tests', new_cluster=False, pipeline_name='pipeline1'))

if __name__ == '__main__':
    unittest.main()

