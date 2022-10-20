from dbx.models.workflow.common.new_cluster import NewCluster


def test_legacy_msg(capsys):
    NewCluster(
        spark_version="some",
        instance_pool_name="some",
        driver_instance_pool_name="some",
        policy_name="some",
        aws_attributes={"instance_profile_name": "some"},
    )
    out = capsys.readouterr().out
    assert "cluster-policy://" in out
    assert "instance-pool://" in out
    assert "driver_instance_pool_id" in out
    assert "instance-profile://" in out
