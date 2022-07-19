from dbx.utils.policy_parser import PolicyParser


def test_base_aws_policy():
    _policy = {
        "aws_attributes.instance_profile_arn": {
            "type": "fixed",
            "value": "arn:aws:iam::123456789:instance-profile/sample-aws-iam",
        },
        "spark_conf.spark.my.conf": {"type": "fixed", "value": "my_value"},
        "spark_conf.spark.my.other.conf": {"type": "fixed", "value": "my_other_value"},
        "init_scripts.0.dbfs.destination": {"type": "fixed", "value": "dbfs:/some/init-scripts/sc1.sh"},
        "init_scripts.1.dbfs.destination": {"type": "fixed", "value": "dbfs:/some/init-scripts/sc2.sh"},
    }
    _formatted = {
        "aws_attributes": {"instance_profile_arn": "arn:aws:iam::123456789:instance-profile/sample-aws-iam"},
        "spark_conf": {"spark.my.conf": "my_value", "spark.my.other.conf": "my_other_value"},
        "init_scripts": [
            {"dbfs": {"destination": "dbfs:/some/init-scripts/sc1.sh"}},
            {"dbfs": {"destination": "dbfs:/some/init-scripts/sc2.sh"}},
        ],
    }
    parser = PolicyParser(_policy)
    _parsed = parser.parse()
    assert _formatted == _parsed
