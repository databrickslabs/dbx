
from databrickslabs_mlflowdepl import dev_cicd_pipeline
from databrickslabs_mlflowdepl import release_cicd_pipeline
from databrickslabs_mlflowdepl import run_now
from databrickslabs_mlflowdepl import deployment
import unittest

from deployment import check_if_dir_is_pipeline_def


class TestMlflowDeployments(unittest.TestCase):
    def test_exec_context_lib(self):
        run_now.main('dev-tests','pipeline1',"0426-172241-jamb3")

if __name__ == '__main__':
    unittest.main()
