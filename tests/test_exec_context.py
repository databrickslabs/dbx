
from databrickslabs_mlflowdepl import dev_cicd_pipeline
from databrickslabs_mlflowdepl import release_cicd_pipeline
from databrickslabs_mlflowdepl import run_now
from databrickslabs_mlflowdepl import deployment
import unittest

from deployment import check_if_dir_is_pipeline_def


class TestMlflowDeployments(unittest.TestCase):
    def test_exec_context_lib(self):
        run_now.main('dev-tests','pipeline1',"0426-172241-jamb3")



    def test_exec_context(self):
        str = """
            %pip install petastorm==0.8.2 pyarrow==0.16.0
            %pip uninstall -y pyspark
            """

        client = deployment.getDatabricksAPIClient()
        print(client.url)
        client.url = 'https://demo.cloud.databricks.com/api/1.2'
        res = client.perform_query(method='POST', path='/contexts/create', data={"language": "python", "clusterId": "0422-085103-lei194"})
        print(res)
        ctx_id = res['id']
        res = client.perform_query(method='POST', path='/commands/execute',
                                   data={"language": "python", "clusterId": "0422-085103-lei194", "contextId" : ctx_id, "command": str})
        print(res)
        cmd_id = res['id']
        import time
        time.sleep(60)
        res = client.perform_query(method='GET', path='/commands/status',
                           data={ "clusterId": "0422-085103-lei194", "contextId" : ctx_id, "commandId": cmd_id})
        print(res)

    def test_res(self):
        client = deployment.getDatabricksAPIClient()
        client.url = 'https://demo.cloud.databricks.com/api/1.2'
        res = client.perform_query(method='GET', path='/commands/status',
                           data={ "clusterId": "0422-085103-lei194", "contextId" : "2346269340379196966", "commandId": "63bf9e46-44af-4063-854b-fbf60e23f04b"})
        print(res)
if __name__ == '__main__':
    unittest.main()

# Set Spark conf: spark.databricks.conda.condaMagic.enabled true
#%pip install petastorm==0.8.2 pyarrow==0.16.0
#%pip uninstall -y pyspark
