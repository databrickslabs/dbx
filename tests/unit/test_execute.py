import datetime as dt
import unittest
from unittest.mock import patch, Mock

from databricks_cli.sdk import ApiClient, ClusterService
from mlflow import ActiveRun
from mlflow.entities import Experiment
from mlflow.entities.run import Run, RunInfo, RunData

from dbx.commands.configure import configure
from dbx.commands.execute import execute, awake_cluster  # noqa
from dbx.utils.common import _preprocess_cluster_args
from .utils import DbxTest, invoke_cli_runner, test_dbx_config

run_info = RunInfo(
    run_uuid="1",
    experiment_id="1",
    user_id="dbx",
    status="STATUS",
    start_time=dt.datetime.now(),
    end_time=dt.datetime.now(),
    lifecycle_stage="STAGE",
    artifact_uri="dbfs:/dbx.unique.uri",
)
run_data = RunData()
run_mock = ActiveRun(Run(run_info, run_data))


class ExecuteTest(DbxTest):
    @patch(
        "databricks_cli.configure.provider.ProfileConfigProvider.get_config",
        return_value=test_dbx_config,
    )
    @patch("databricks_cli.workspace.api.WorkspaceService.mkdirs", return_value=True)
    @patch("databricks_cli.sdk.service.DbfsService.get_status", return_value=True)
    @patch(
        "databricks_cli.clusters.api.ClusterService.get_cluster",
        return_value={"cluster_name": "some-name", "state": "RUNNING"},
    )
    @patch("dbx.utils.v1_client.ApiV1Client.create_context", return_value={"id": 1})
    @patch("dbx.utils.v1_client.ApiV1Client.execute_command", return_value={"id": 1})
    @patch(
        "dbx.utils.v1_client.ApiV1Client.get_command_status",
        return_value={
            "status": "Finished",
            "results": {"resultType": "Ok", "data": "Ok!"},
        },
    )
    @patch("mlflow.start_run", return_value=run_mock)
    @patch("mlflow.tracking.fluent.end_run", return_value=None)
    @patch("mlflow.log_artifact", return_value=None)
    @patch("mlflow.set_tags", return_value=None)
    @patch(
        "mlflow.get_experiment_by_name",
        return_value=Experiment("id", None, "location", None, None),
    )
    @patch("mlflow.set_experiment", return_value=None)
    def test_execute(self, *args):  # noqa
        with self.project_dir:
            ws_dir = "/Shared/dbx/projects/%s" % self.project_name
            configure_result = invoke_cli_runner(
                configure,
                [
                    "--environment",
                    "test",
                    "--profile",
                    self.profile_name,
                    "--workspace-dir",
                    ws_dir,
                ],
            )
            self.assertEqual(configure_result.exit_code, 0)

            with patch(
                "mlflow.get_experiment_by_name",
                return_value=Experiment("id", None, f"dbfs:/Shared/dbx/projects/{self.project_name}", None, None),
            ):
                execute_result = invoke_cli_runner(
                    execute,
                    [
                        "--deployment-file",
                        "conf/deployment.yml",
                        "--environment",
                        "default",
                        "--cluster-id",
                        "000-some-cluster-id",
                        "--job",
                        f"{self.project_name}-sample-etl-2.0",
                    ],
                )

                self.assertEqual(execute_result.exit_code, 0)

    @patch(
        "databricks_cli.clusters.api.ClusterService.list_clusters",
        return_value={
            "clusters": [
                {"cluster_name": "some-cluster-name", "cluster_id": "aaa-111"},
                {"cluster_name": "other-cluster-name", "cluster_id": "aaa-bbb-ccc"},
                {"cluster_name": "duplicated-name", "cluster_id": "duplicated-1"},
                {"cluster_name": "duplicated-name", "cluster_id": "duplicated-2"},
            ]
        },
    )
    @patch(
        "databricks_cli.clusters.api.ClusterService.get_cluster",
        side_effect=lambda cid: "something" if cid in ("aaa-bbb-ccc", "aaa-111") else None,
    )
    def test_preprocess_cluster_args(self, *args):  # noqa
        api_client = Mock(ApiClient)

        self.assertRaises(RuntimeError, _preprocess_cluster_args, api_client, None, None)

        id_by_name = _preprocess_cluster_args(api_client, "some-cluster-name", None)
        self.assertEqual(id_by_name, "aaa-111")

        id_by_id = _preprocess_cluster_args(api_client, None, "aaa-bbb-ccc")
        self.assertEqual(id_by_id, "aaa-bbb-ccc")

        self.assertRaises(NameError, _preprocess_cluster_args, api_client, "non-existent-cluster-by-name", None)
        self.assertRaises(NameError, _preprocess_cluster_args, api_client, "duplicated-name", None)
        self.assertRaises(NameError, _preprocess_cluster_args, api_client, None, "non-existent-id")

    def test_awake_cluster(self):
        # normal behavior
        cluster_service_mock = Mock(ClusterService)
        cluster_service_mock.get_cluster.side_effect = [
            {"state": "TERMINATED"},
            {"state": "PENDING"},
            {"state": "RUNNING"},
            {"state": "RUNNING"},
        ]
        awake_cluster(cluster_service_mock, "aaa-bbb")
        self.assertEqual(cluster_service_mock.get_cluster("aaa-bbb").get("state"), "RUNNING")

        # error behavior
        error_mock = Mock(ClusterService)
        error_mock.get_cluster.return_value = {"state": "ERROR"}
        self.assertRaises(RuntimeError, awake_cluster, error_mock, "aaa-bbb")


if __name__ == "__main__":
    unittest.main()
