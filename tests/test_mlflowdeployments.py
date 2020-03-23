from databrickslabs_mlflowdepl import dev_cicd_pipeline
from databrickslabs_mlflowdepl import release_cicd_pipeline
import unittest

class TestMlflowDeployments(unittest.TestCase):
    def test_dev_cicd_pipeline(self):
        dev_cicd_pipeline.main('dev-tests')

    def test_create_prod_jobs(self):
        release_cicd_pipeline.main('','dev-tests',False)

if __name__ == '__main__':
    unittest.main()