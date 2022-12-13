from unittest.mock import MagicMock

import pytest
import yaml
from databricks_cli.sdk import PolicyService
from pytest_mock import MockerFixture

from dbx.api.adjuster.adjuster import Adjuster, AdditionalLibrariesProvider
from dbx.api.adjuster.policy import PolicyAdjuster
from dbx.models.deployment import DeploymentConfig
from dbx.models.workflow.common.libraries import Library
from dbx.models.workflow.common.new_cluster import NewCluster


def test_base_aws_policy():
    _policy = {
        "aws_attributes.instance_profile_arn": {
            "type": "fixed",
            "value": "arn:aws:iam::123456789:instance-profile/sample-aws-iam",
        },
        "spark_version": {"type": "fixed", "value": "lts"},
        "node_type_id": {"type": "allowlist", "values": ["node_1", "node_2"]},
        "spark_conf.spark.my.conf": {"type": "fixed", "value": "my_value"},
        "spark_conf.spark.my.other.conf": {"type": "fixed", "value": "my_other_value"},
        "init_scripts.0.dbfs.destination": {"type": "fixed", "value": "dbfs:/some/init-scripts/sc1.sh"},
        "init_scripts.1.dbfs.destination": {"type": "fixed", "value": "dbfs:/some/init-scripts/sc2.sh"},
    }
    _formatted = {
        "aws_attributes": {"instance_profile_arn": "arn:aws:iam::123456789:instance-profile/sample-aws-iam"},
        "spark_conf": {"spark.my.conf": "my_value", "spark.my.other.conf": "my_other_value"},
        "spark_version": "lts",
        "init_scripts": [
            {"dbfs": {"destination": "dbfs:/some/init-scripts/sc1.sh"}},
            {"dbfs": {"destination": "dbfs:/some/init-scripts/sc2.sh"}},
        ],
    }
    api_client = MagicMock()
    adj = PolicyAdjuster(api_client)
    result = adj._traverse_policy(_policy)
    assert result == _formatted


@pytest.fixture()
def policy_mock(mocker: MockerFixture):
    mocker.patch.object(
        PolicyService,
        "list_policies",
        MagicMock(
            return_value={
                "policies": [
                    {
                        "policy_id": 1,
                        "name": "good-policy",
                        "definition": """
                        {"spark_conf.spark.my.conf": {"type": "fixed", "value": "my_value"}}
                        """,
                    },
                    {
                        "policy_id": 2,
                        "name": "duplicated-name",
                        "definition": """
                        {"spark_conf.spark.my.conf": {"type": "fixed", "value": "my_value"}}
                        """,
                    },
                    {
                        "policy_id": 3,
                        "name": "duplicated-name",
                        "definition": """
                        {"spark_conf.spark.my.conf": {"type": "fixed", "value": "my_value"}}
                        """,
                    },
                    {
                        "policy_id": 4,
                        "name": "conflicting",
                        "definition": """
                        {"spark_version": {"type": "fixed", "value": "some-other"}}
                        """,
                    },
                    {
                        "policy_id": 20,
                        "name": "policy-with-one-script",
                        "definition": """
                        {
                          "init_scripts.0.dbfs.destination": {
                            "type": "fixed",
                            "value": "dbfs://some/path/script.sh"
                          }
                        }
                        """,
                    },
                    {
                        "policy_id": 10,
                        "name": "policy-with-multiple-scripts",
                        "definition": """
                        {
                          "init_scripts.0.dbfs.destination": {
                            "type": "fixed",
                            "value": "dbfs://some/path/script.sh"
                          },
                          "init_scripts.1.dbfs.destination": {
                            "type": "fixed",
                            "value": "dbfs://some/path/other-script.sh"
                          }
                        }
                        """,
                    },
                ]
            }
        ),
    )


@pytest.mark.parametrize(
    "cluster_def",
    [
        NewCluster(spark_version="lts", policy_name="good-policy"),
        NewCluster(spark_version="lts", policy_id="cluster-policy://good-policy"),
    ],
)
def test_adjusting(cluster_def, policy_mock):
    _adj = PolicyAdjuster(api_client=MagicMock())
    _obj = _adj._adjust_policy_ref(cluster_def)
    assert getattr(_obj, "spark_conf").get("spark.my.conf") == "my_value"


@pytest.mark.parametrize(
    "cluster_def",
    [
        NewCluster(spark_version="lts", policy_id="cluster-policy://duplicated-name"),
        NewCluster(spark_version="lts", policy_id="cluster-policy://not-found"),
        NewCluster(spark_version="lts", policy_id="cluster-policy://conflicting"),
    ],
)
def test_negative_cases(cluster_def, policy_mock):
    _adj = PolicyAdjuster(api_client=MagicMock())
    with pytest.raises(ValueError):
        _obj = _adj._adjust_policy_ref(cluster_def)


TEST_DEFINITIONS = yaml.safe_load(
    """
environments:
  default:
    workflows:
      - name: "legacy-definition"
        some_task: "here"
        new_cluster:
          spark_version: "some"
          policy_id: "cluster-policy://good-policy"
      - name: "v2.1-inplace"
        job_clusters:
          - job_cluster_key: "base"
            new_cluster:
              spark_version: "some"
              policy_id: "cluster-policy://good-policy"
        tasks:
          - task_key: "inplace"
            new_cluster:
              spark_version: "some"
              policy_id: "cluster-policy://good-policy"
            some_task: "here"
          - task_key: "from-job-clusters"
            job_cluster_key: "base"
            some_task: "here"
"""
)

TEST_CONFIG = DeploymentConfig.from_payload(TEST_DEFINITIONS)
ENVIRONMENT_DEFINITION = TEST_CONFIG.get_environment("default")


def test_locations(policy_mock):
    wfs = TEST_CONFIG.get_environment("default").payload.workflows
    core_pkg = Library(whl="/some/local/file")
    client_mock = MagicMock()
    _adj = Adjuster(
        additional_libraries=AdditionalLibrariesProvider(core_package=core_pkg),
        file_uploader=MagicMock(),
        api_client=client_mock,
    )
    _adj.traverse(wfs)
    for element in [
        wfs[0].new_cluster,
        wfs[1].get_task("inplace").new_cluster,
        wfs[1].get_job_cluster_definition("base").new_cluster,
    ]:
        assert getattr(element, "spark_conf").get("spark.my.conf") == "my_value"
        assert element.policy_id == "1"


@pytest.mark.parametrize(
    "existing_init_scripts, expected",
    [
        (
            [],
            [
                {"dbfs": {"destination": "dbfs1"}},
                {"dbfs": {"destination": "dbfs2"}},
                {"s3": {"destination": "s31"}},
                {"s3": {"destination": "s32"}},
            ],
        ),
        (
            [
                {"dbfs": {"destination": "dbfs1"}},
                {"dbfs": {"destination": "dbfs2"}},
                {"s3": {"destination": "s31"}},
                {"s3": {"destination": "s32"}},
            ],
            [
                {"dbfs": {"destination": "dbfs1"}},
                {"dbfs": {"destination": "dbfs2"}},
                {"s3": {"destination": "s31"}},
                {"s3": {"destination": "s32"}},
            ],
        ),
        (
            [
                {"dbfs": {"destination": "dbfs2"}},
                {"dbfs": {"destination": "dbfs3"}},
                {"s3": {"destination": "s32"}},
                {"s3": {"destination": "s33"}},
            ],
            [
                {"dbfs": {"destination": "dbfs1"}},
                {"dbfs": {"destination": "dbfs2"}},
                {"s3": {"destination": "s31"}},
                {"s3": {"destination": "s32"}},
                {"dbfs": {"destination": "dbfs3"}},
                {"s3": {"destination": "s33"}},
            ],
        ),
    ],
)
def test_append_init_scripts(existing_init_scripts, expected):
    policy_init_scripts = [
        {"dbfs": {"destination": "dbfs1"}},
        {"dbfs": {"destination": "dbfs2"}},
        {"s3": {"destination": "s31"}},
        {"s3": {"destination": "s32"}},
    ]
    assert expected == PolicyAdjuster._append_init_scripts(policy_init_scripts, existing_init_scripts)


TESTS_WITH_SCRIPTS = yaml.safe_load(
    """
environments:
  default:
    workflows:
      - name: "one-script"
        some_task: "here"
        new_cluster:
          spark_version: "some"
          policy_id: "cluster-policy://policy-with-one-script"
          init_scripts:
            - dbfs:
                destination: "dbfs:/some/script.sh"
      - name: "multiple-scripts"
        some_task: "here"
        new_cluster:
          spark_version: "some"
          policy_id: "cluster-policy://policy-with-multiple-scripts"
          init_scripts:
            - dbfs:
                destination: "dbfs:/some/script.sh"
      - name: "wrong-format"
        some_task: "here"
        new_cluster:
          spark_version: "some"
          policy_id: "cluster-policy://policy-with-one-script"
          init_scripts:
            - dbfs:
              destination: "dbfs:/some/script.sh"
"""
)


@pytest.mark.parametrize(
    "wf_name, amount_or_behaviour", [("one-script", 2), ("multiple-scripts", 3), ("wrong-format", Exception())]
)
def test_with_scripts(wf_name, amount_or_behaviour, policy_mock):
    wf = DeploymentConfig.from_payload(TESTS_WITH_SCRIPTS).get_environment("default").payload.get_workflow(wf_name)
    core_pkg = Library(whl="/some/local/file")
    client_mock = MagicMock()
    _adj = Adjuster(
        additional_libraries=AdditionalLibrariesProvider(core_package=core_pkg),
        file_uploader=MagicMock(),
        api_client=client_mock,
    )
    if isinstance(amount_or_behaviour, Exception):
        with pytest.raises(Exception):
            _adj.traverse([wf])
    else:
        _adj.traverse([wf])
        assert not wf.new_cluster.policy_id.startswith("cluster-policy://")
        assert len(getattr(wf.new_cluster, "init_scripts")) == amount_or_behaviour
