from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from dbx.api.destroyer import Destroyer, WorkflowEraser, AssetEraser
from dbx.models.deployment import EnvironmentDeploymentInfo
from dbx.models.destroyer import DestroyerConfig, DeletionMode


@pytest.mark.parametrize(
    "mode, dracarys, wf_expected, assets_expected, dracarys_expected",
    [
        (DeletionMode.all, True, True, True, True),
        (DeletionMode.assets_only, True, False, True, True),
        (DeletionMode.workflows_only, False, True, False, False),
    ],
)
def test_destroyer_modes(
    mode, dracarys, wf_expected, assets_expected, dracarys_expected, temp_project, capsys, mocker: MockerFixture
):
    api_client = MagicMock()
    d_info = EnvironmentDeploymentInfo(name="default", payload={"workflows": [{"name": "w1"}]})
    _dc = DestroyerConfig(deletion_mode=mode, deployment=d_info, dracarys=dracarys)
    wf_mock = mocker.patch.object(WorkflowEraser, "erase", MagicMock())
    assets_mock = mocker.patch.object(AssetEraser, "erase", MagicMock())
    d = Destroyer(api_client, _dc)
    d.launch()
    dracarys_test = "🔥🔥🔥" in capsys.readouterr().out

    assert wf_mock.called == wf_expected
    assert assets_mock.called == assets_expected
    assert dracarys_test == dracarys_expected


# def test_destroyer_dry_run(temp_project, capsys):
#     client_mock = MagicMock()
#     d_info = EnvironmentDeploymentInfo(name="default", payload={"workflows": [{"name": "w1"}]})
#     _dc = DestroyerConfig(deletion_mode=DeletionMode.all, dry_run=True, deployment=d_info)
#     _d = Destroyer(client_mock, _dc)
#     _d.launch()
#     assert client_mock.call_count == 0
