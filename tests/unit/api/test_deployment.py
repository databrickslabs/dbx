from unittest.mock import MagicMock

import yaml
from pytest_mock import MockerFixture

from dbx.api.deployment import WorkflowDeploymentManager
from dbx.api.services.jobs import NamedJobsService
from dbx.api.services.pipelines import NamedPipelinesService
from dbx.models.deployment import EnvironmentDeploymentInfo

TEST_PAYLOAD = """
    workflows:
      - name: "some-wf"
        access_control_list:
          - user_name: "some@email.com"
            permission_level: "IS_OWNER"
        tasks:
          - task_key: "some-task"
            python_wheel_task:
              entry_point: "some-ep"
              package_name: "some-pkg"
      - name: "some-pipeline"
        workflow_type: "pipeline"
        target: "target_db"
        libraries:
          - notebook:
              path: "/Some/path/in/repos"
    """

WFS = EnvironmentDeploymentInfo.from_spec("default", yaml.safe_load(TEST_PAYLOAD))


def test_basic():
    mgr = WorkflowDeploymentManager(api_client=MagicMock(), workflows=WFS.payload.workflows)
    mgr.apply()


def test_update_basic(mocker: MockerFixture):
    mocker.patch.object(NamedJobsService, "find_by_name", MagicMock(return_value=1))
    mocker.patch.object(NamedPipelinesService, "find_by_name", MagicMock(return_value="aaa-bbb"))
    mgr = WorkflowDeploymentManager(api_client=MagicMock(), workflows=WFS.payload.workflows)
    mgr.apply()
