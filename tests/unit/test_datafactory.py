from unittest.mock import MagicMock, patch

from dbx.api.client_provider import DatabricksClientProvider
from dbx.commands.configure import configure
from dbx.commands.datafactory import reflect
from dbx.commands.deploy import deploy, _update_job, _log_dbx_file  # noqa
from .conftest import invoke_cli_runner, extract_function_name


def test_datafactory_deploy(mocker, temp_project):
    mocker.patch.object(DatabricksClientProvider, "get_v2_client", MagicMock())
    func = _log_dbx_file
    mocker.patch(extract_function_name(func), MagicMock())

    ws_dir = "/Shared/dbx/projects/%s" % temp_project.name
    configure_result = invoke_cli_runner(
        configure,
        [
            "--environment",
            "default",
            "--profile",
            temp_project.name,
            "--workspace-dir",
            ws_dir,
        ],
    )
    assert configure_result.exit_code == 0

    deploy_result = invoke_cli_runner(
        deploy,
        [
            "--deployment-file",
            "conf/deployment.yml",
            "--environment",
            "default",
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
