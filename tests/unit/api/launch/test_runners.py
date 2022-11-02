from unittest.mock import MagicMock

import pytest
from databricks_cli.sdk import JobsService, ApiClient
from pytest_mock import MockerFixture

from dbx.api.launch.runners.standard import StandardLauncher
from dbx.api.services.jobs import NamedJobsService
from dbx.models.cli.options import ExistingRunsOption


def test_not_found(mocker: MockerFixture):
    mocker.patch.object(NamedJobsService, "find_by_name", MagicMock(return_value=None))
    launcher = StandardLauncher("non-existent", api_client=MagicMock(), existing_runs=ExistingRunsOption.pass_)
    with pytest.raises(Exception):
        launcher.launch()


@pytest.mark.parametrize(
    "behaviour, msg",
    [
        (ExistingRunsOption.pass_, "Passing the existing"),
        (ExistingRunsOption.wait, "Waiting for job run"),
        (ExistingRunsOption.cancel, "Cancelling run"),
    ],
)
def test_with_behaviours(behaviour, msg, mocker: MockerFixture, capsys):
    mocker.patch("dbx.api.launch.runners.standard.wait_run", MagicMock())
    mocker.patch("dbx.api.launch.runners.standard.cancel_run", MagicMock())
    client = MagicMock()
    client.perform_query = MagicMock(return_value={"run_id": 1})
    mocker.patch.object(NamedJobsService, "find_by_name", MagicMock(return_value=1))
    mocker.patch.object(JobsService, "list_runs", MagicMock(return_value={"runs": [{"run_id": 1}]}))

    launcher = StandardLauncher("non-existent", api_client=client, existing_runs=behaviour)
    launcher.launch()
    assert msg in capsys.readouterr().out
