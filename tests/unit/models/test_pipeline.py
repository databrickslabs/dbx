from dbx.models.workflow.common.pipeline import PipelinesNewCluster


def test_omits(capsys):
    nc = PipelinesNewCluster(spark_version="some", init_scripts=["a", "b"])
    _out = capsys.readouterr().out
    assert "The `spark_version` property cannot be applied" in _out
    assert "The `init_scripts` property cannot be applied" in _out
    assert nc.init_scripts == []
    assert nc.spark_version is None
