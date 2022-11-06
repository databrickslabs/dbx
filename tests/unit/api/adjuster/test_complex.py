from unittest.mock import MagicMock

import pytest
import yaml
from databricks_cli.sdk import InstancePoolService
from pytest_mock import MockerFixture

from dbx.api.adjuster.adjuster import Adjuster, AdditionalLibrariesProvider
from dbx.api.services.pipelines import NamedPipelinesService
from dbx.models.deployment import DeploymentConfig
from dbx.models.workflow.common.libraries import Library
from dbx.models.workflow.common.workflow_types import WorkflowType

TEST_PAYLOAD = yaml.safe_load(
    """
custom:
  basic-cluster-props: &basic-cluster-props
    spark_version: "9.1.x-cpu-ml-scala2.12"

  basic-static-cluster: &basic-static-cluster
    new_cluster:
      <<: *basic-cluster-props
      num_workers:  1
      instance_pool_id: "instance-pool://pool-2"
      instance_pool_id: "instance-pool://pool-2"

environments:
  default:
    workflows:

      - name: "dlt-test"
        workflow_type: "pipeline"
        deployment_config:
          no_package: true
        target: "some"
        libraries:
          - notebook:
              path: "/Repos/some"
        clusters:
          - label: "default"
            autoscale:
              min_workers: 1
              max_workers: 3
              mode: "ENHANCED"
            instance_pool_id: "instance-pool://pool-1"
            driver_instance_pool_id: "instance-pool://pool-1"


      - name: "dbx-pipeline-chain"
        access_control_list:
          - user_name: "some@email.com"
            permission_level: "IS_OWNER"
        job_clusters:
          - job_cluster_key: "main"
            <<: *basic-static-cluster
        tasks:
          - task_key: "first"
            job_cluster_key: "main"
            python_wheel_task:
              entry_point: "etl"
              package_name: "dbx_exec_srv"
          - task_key: "second"
            deployment_config:
              no_package: true
            pipeline_task:
              pipeline_id: "pipeline://dlt-test"
"""
)

TEST_CONFIG = DeploymentConfig.from_payload(TEST_PAYLOAD)
ENVIRONMENT_DEFINITION = TEST_CONFIG.get_environment("default")


@pytest.fixture
def complex_instance_pool_mock(mocker: MockerFixture):
    mocker.patch.object(
        InstancePoolService,
        "list_instance_pools",
        MagicMock(
            return_value={
                "instance_pools": [
                    {"instance_pool_name": "pool-1", "instance_pool_id": "some-id-1"},
                    {"instance_pool_name": "pool-2", "instance_pool_id": "some-id-2"},
                ]
            }
        ),
    )


@pytest.fixture
def named_pipeline_mock(mocker: MockerFixture):
    mocker.patch.object(NamedPipelinesService, "find_by_name_strict", MagicMock(return_value="aa-bb"))


def test_complex(complex_instance_pool_mock, named_pipeline_mock):
    wfs = TEST_CONFIG.get_environment("default").payload.workflows
    core_pkg = Library(whl="/some/local/file")
    client_mock = MagicMock()
    _adj = Adjuster(
        additional_libraries=AdditionalLibrariesProvider(core_package=core_pkg),
        file_uploader=MagicMock(),
        api_client=client_mock,
    )
    _adj.traverse(wfs)

    assert wfs[0].workflow_type == WorkflowType.pipeline
    assert wfs[0].clusters[0].instance_pool_id == "some-id-1"
    assert wfs[0].clusters[0].driver_instance_pool_id == "some-id-1"
    assert len(wfs[0].libraries) == 1
    assert wfs[1].workflow_type == WorkflowType.job_v2d1
    assert core_pkg in wfs[1].get_task("first").libraries
    assert core_pkg not in wfs[1].get_task("second").libraries


TEST_ARBITRARY_TRVS_PAYLOAD = yaml.safe_load(
    """
environments:
  default:
    workflows:
      - name: "sample-wf"
        tags:
          key: "instance-pool://pool-1"
        tasks:
         - task_key: "some-task"
           some_task:
             list_props:
               - "instance-pool://pool-1"
             nested_props:
               nested_key: "instance-pool://pool-1"
"""
)


def test_arbitrary_traversals(complex_instance_pool_mock):
    default = DeploymentConfig.from_payload(TEST_ARBITRARY_TRVS_PAYLOAD).get_environment("default")
    _adj = Adjuster(
        additional_libraries=AdditionalLibrariesProvider(core_package=None),
        file_uploader=MagicMock(),
        api_client=MagicMock(),
    )
    _adj.traverse(default.payload.workflows)
    _wf = default.payload.get_workflow("sample-wf")
    assert _wf.tags["key"] == "some-id-1"
    assert getattr(_wf.get_task("some-task"), "some_task")["list_props"][0] == "some-id-1"
    assert getattr(_wf.get_task("some-task"), "some_task")["nested_props"]["nested_key"] == "some-id-1"
