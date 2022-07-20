from unittest.mock import MagicMock

from databricks_cli.sdk import JobsService

from dbx.api.output_provider import OutputProvider


def test_provider_empty(capsys):
    js = MagicMock()
    provider = OutputProvider(js, {})
    provider.provide("stdout")
    captured = capsys.readouterr()
    assert "cannot be captured since the job is not based on Jobs API V2.X+" in captured.out


def test_provider_full(capsys):
    js = JobsService(MagicMock())
    js.get_run_output = MagicMock(
        return_value={
            "logs": "some line \n some other line",
            "error": "some line \n some other line",
            "error_trace": "some line \n some other line",
        }
    )
    provider = OutputProvider(js, {"tasks": [{"run_id": 1, "task_key": "test"}]})
    provider.provide("stdout")
    captured = capsys.readouterr()
    assert "stdout end" in captured.out
    assert "stderr end" in captured.out
    assert "error trace end" in captured.out


def test_provider_logs_missing(capsys):
    js = JobsService(MagicMock())
    js.get_run_output = MagicMock(
        return_value={"error": "some line \n some other line", "error_trace": "some line \n some other line"}
    )
    provider = OutputProvider(js, {"tasks": [{"run_id": 1, "task_key": "test"}]})
    provider.provide("stdout")
    captured = capsys.readouterr()
    assert "stderr end" in captured.out
    assert "No stdout provided in the run output" in captured.out
