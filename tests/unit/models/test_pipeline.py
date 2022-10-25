import yaml

from dbx.models.deployment import Deployment
from dbx.models.workflow.common.pipeline import Pipeline
from dbx.models.workflow.v2dot1.workflow import Workflow as V2dot1Workflow

TEST_PAYLOAD = """
workflows:
- name: "dlt-pipeline"
  workflow_type: "pipeline"
- name: "job-v21"
  tasks:
    - task_key: "first"
      spark_python_task:
        python_file: "/some/file"
"""


def test_various_workflow_definitions():
    _dep = Deployment.from_spec_remote(yaml.safe_load(TEST_PAYLOAD))
    assert isinstance(_dep.get_workflow("dlt-pipeline"), Pipeline)
    assert isinstance(_dep.get_workflow("job-v21"), V2dot1Workflow)
