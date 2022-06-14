import json
from unittest.mock import MagicMock, Mock

import pytest

from dbx.api.clients.databricks_api import DatabricksClientProvider
from dbx.models.base import FlexibleBaseModel
from dbx.models.deployment import Deployment, WorkloadDefinition
from dbx.models.tasks import DependencyDefinition, TaskDefinition
from dbx.utils.adjuster import AdjustmentManager
from dbx.utils.named_properties import NewClusterPropertiesProcessor, PolicyNameProcessor

SAMPLE_DEPLOYMENT = Deployment(
    **{
        "environments": {
            "default": {
                "workloads": [
                    {
                        "name": "main",
                        "tasks": [
                            {"task_key": "first", "spark_python_task": {"python_file": "file://some-file.py"}},
                            {
                                "task_key": "second",
                                "notebook_task": {"notebook_path": "/Repos/some-project/notebook"},
                                "max_retries": "10",
                                "depends_on": [{"task_key": "first"}],
                            },
                            {
                                "task_key": "third",
                                "spark_submit_task": {"parameters": ["some", "parameter"]},
                                "depends_on": [{"task_key": "first"}, {"task_key": "second"}],
                            },
                        ],
                    }
                ]
            }
        }
    }
)


def test_flexible_base_model():
    class TestingModel(FlexibleBaseModel):
        id: str

    extras = {"extra_field": 10, "nested_extra_field": [1, 2, 3]}
    td = TestingModel(id="1", **extras)
    assert td.extra == extras


def test_positive_model():
    assert len(SAMPLE_DEPLOYMENT.environments) == 1
    assert len(SAMPLE_DEPLOYMENT.environments["default"].workloads) == 1
    assert SAMPLE_DEPLOYMENT.environments["default"].workloads[0].name == "main"
    assert len(SAMPLE_DEPLOYMENT.environments["default"].workloads[0].tasks) == 3
    assert SAMPLE_DEPLOYMENT.environments["default"].workloads[0].tasks[0].task_key == "first"
    assert SAMPLE_DEPLOYMENT.environments["default"].workloads[0].tasks[1].extra == {"max_retries": "10"}
    assert SAMPLE_DEPLOYMENT.environments["default"].workloads[0].tasks[2].depends_on == [
        DependencyDefinition(task_key="first"),
        DependencyDefinition(task_key="second"),
    ]


def test_model_transformation(mocker):
    before = [
        WorkloadDefinition(
            **{
                "name": "test",
                "job_clusters": [
                    {
                        "job_cluster_key": "first",
                        "new_cluster": {
                            "aws_attributes": {"instance_profile_name": "sample"},
                            "driver_instance_pool_name": "sample",
                            "instance_pool_name": "sample",
                            "policy_name": "sample_policy",
                            "init_scripts": {"dbfs": {"destination": "file://some-file.sh"}},
                        },
                    },
                ],
                "tasks": [{"task_key": "first", "spark_python_task": {"python_file": "file://some/local/path"}}],
            }
        )
    ]
    uploader = MagicMock()
    uploader.upload_and_provide_path = Mock(return_value="dbfs:/some/path")
    mocker.patch.object(DatabricksClientProvider, "get_v2_client", return_value=MagicMock())
    mocker.patch.object(AdjustmentManager, "_verify_file_exists", return_value=MagicMock())
    mocker.patch.object(
        NewClusterPropertiesProcessor,
        "_list_instance_profiles",
        return_value=[{"instance_profile_arn": "aws:iam:arn:123456789/sample"}],
    )
    mocker.patch.object(
        NewClusterPropertiesProcessor,
        "_list_instance_pools",
        return_value=[{"instance_pool_name": "sample", "instance_pool_id": "123-abc"}],
    )
    mocker.patch.object(
        PolicyNameProcessor,
        "_list_policies",
        return_value=[
            {
                "name": "sample_policy",
                "definition": json.dumps(
                    {
                        "init_scripts.0.dbfs.destination": {
                            "type": "fixed",
                            "value": "dbfs:/Shared/init-scripts/sample-script.sh",
                        }
                    }
                ),
            }
        ],
    )

    manager = AdjustmentManager(before, uploader)
    after = manager.adjust_workloads()


def test_negative_task_model():
    with pytest.raises(Exception):
        _ = TaskDefinition(**{"task_key": "something"})
