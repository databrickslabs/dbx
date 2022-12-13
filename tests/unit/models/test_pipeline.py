from dbx.models.workflow.common.pipeline import PipelinesNewCluster


def test_omits(capsys):
    nc = PipelinesNewCluster(spark_version="some")
    _out = capsys.readouterr().out
    assert "The `spark_version` property cannot be applied" in _out
    assert nc.spark_version is None
