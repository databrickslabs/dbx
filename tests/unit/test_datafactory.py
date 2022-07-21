from pathlib import Path
from unittest.mock import MagicMock, patch

from dbx.api.client_provider import DatabricksClientProvider
from dbx.commands.datafactory import reflect
from dbx.commands.deploy import deploy, _log_dbx_file
from .conftest import invoke_cli_runner, extract_function_name


def test_datafactory_deploy(mocker, temp_project: Path, mlflow_file_uploader):
    mocker.patch.object(DatabricksClientProvider, "get_v2_client", MagicMock())
    func = _log_dbx_file
    mocker.patch(extract_function_name(func), MagicMock())
    deploy_result = invoke_cli_runner(
        deploy,
        [
            "--write-specs-to-file",
            ".dbx/deployment-result.json",
        ],
    )

    assert deploy_result.exit_code == 0

    with patch("dbx.commands.datafactory.DatafactoryReflector", autospec=True):
        reflection_result = invoke_cli_runner(
            reflect,
            [
                "--environment",
                "default",
                "--specs-file",
                ".dbx/deployment-result.json",
                "--subscription-name",
                "some-subscription",
                "--resource-group",
                "some-resource-group",
                "--factory-name",
                "some-factory",
                "--name",
                "some-pipeline",
            ],
        )

        assert reflection_result.exit_code == 0
