import time
from functools import partial
from unittest.mock import MagicMock

import mlflow
import pytest
from pytest_mock import MockerFixture

from dbx.api.destroyer import Destroyer, WorkflowEraser, AssetEraser
from dbx.api.services.jobs import NamedJobsService
from dbx.models.cli.destroyer import DestroyerConfig, DeletionMode
from dbx.models.deployment import EnvironmentDeploymentInfo
from dbx.models.workflow.v2dot1.workflow import Workflow
from tests.unit.conftest import invoke_cli_runner

test_env_info = EnvironmentDeploymentInfo(
    name="default", payload={"workflows": [{"name": "w1", "workflow_type": "job-v2.0", "some_task": "t"}]}
)
basic_config = partial(DestroyerConfig, deployment=test_env_info)


@pytest.mark.parametrize(
    "conf, wf_expected, assets_expected",
    [
        (basic_config(deletion_mode=DeletionMode.all, dracarys=True, dry_run=True), True, True),
        (basic_config(deletion_mode=DeletionMode.workflows_only, dracarys=False, dry_run=False), True, False),
        (basic_config(deletion_mode=DeletionMode.assets_only, dracarys=True, dry_run=False), False, True),
        (basic_config(deletion_mode=DeletionMode.assets_only, dracarys=False, dry_run=True), False, True),
    ],
)
def test_destroyer_modes(conf, wf_expected, assets_expected, temp_project, capsys, mocker: MockerFixture):
    api_client = MagicMock()
    wf_mock = mocker.patch.object(WorkflowEraser, "erase", MagicMock())
    assets_mock = mocker.patch.object(AssetEraser, "erase", MagicMock())
    d = Destroyer(api_client, conf)
    d.launch()
    output = capsys.readouterr().out
    assert wf_mock.called == wf_expected
    assert assets_mock.called == assets_expected

    if not conf.dry_run and conf.dracarys:
        assert "🔥🔥🔥" in output

    if conf.dry_run and conf.dracarys:
        assert "would be displayed here" in output


@pytest.mark.parametrize("dry_run", [True, False])
def test_workflow_eraser_dr(dry_run, capsys, mocker: MockerFixture):
    mocker.patch.object(NamedJobsService, "find_by_name", MagicMock(return_value=1))
    eraser = WorkflowEraser(MagicMock(), [Workflow(name="test", some_task="here")], dry_run=dry_run)
    eraser.erase()
    out = capsys.readouterr().out
    _test = "would be deleted" in out
    assert _test == dry_run


@pytest.mark.parametrize(
    "job_response,expected",
    [
        (None, "doesn't exist"),
        (1, "was successfully deleted"),
    ],
)
def test_workflow_eraser_list(job_response, expected, capsys, mocker: MockerFixture):
    api_client = MagicMock()
    mocker.patch.object(NamedJobsService, "find_by_name", MagicMock(return_value=job_response))
    eraser = WorkflowEraser(api_client, [Workflow(name="test", some_task="here")], dry_run=False)
    if isinstance(expected, Exception):
        with pytest.raises(Exception):
            eraser.erase()
    else:
        eraser.erase()
        out = capsys.readouterr().out
        assert expected in out


@pytest.mark.parametrize("dry_run", [True, False])
def test_asset_eraser_dr(dry_run, tmp_path, capsys, temp_project):
    exp = mlflow.create_experiment(tmp_path.name, tmp_path.as_uri())
    invoke_cli_runner(f"configure --workspace-directory={tmp_path.name} --artifact-location={tmp_path.as_uri()}")

    for i in range(3):
        with mlflow.start_run(experiment_id=exp):
            time.sleep(0.4)  # to avoid race conditions during parallel launch
            mlflow.set_tag("id", 1)

    conf = basic_config(deletion_mode=DeletionMode.assets_only, dry_run=dry_run)

    d = Destroyer(MagicMock(), conf)
    d.launch()
    out = capsys.readouterr().out
    dr_provided = "would be deleted" in out.replace("\n", " ")
    assert dry_run == dr_provided


def test_asset_eraser_no_experiment(tmp_path, capsys, temp_project):
    invoke_cli_runner(f"configure --workspace-directory={tmp_path.name} --artifact-location={tmp_path.as_uri()}")
    conf = basic_config(deletion_mode=DeletionMode.assets_only)

    d = Destroyer(MagicMock(), conf)
    d.launch()
    out = capsys.readouterr().out
    assert "non-existent mlflow experiment" in out.replace("\n", "")


def test_asset_eraser_no_runs(tmp_path, capsys, temp_project):
    mlflow.create_experiment(tmp_path.name, tmp_path.as_uri())
    invoke_cli_runner(f"configure --workspace-directory={tmp_path.name} --artifact-location={tmp_path.as_uri()}")
    conf = basic_config(deletion_mode=DeletionMode.assets_only)

    d = Destroyer(MagicMock(), conf)
    d.launch()
