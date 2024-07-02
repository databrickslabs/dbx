from typing import Optional

from pydantic import root_validator, validator

from dbx.models.workflow.common.flexible import FlexibleModel


class AutoScale(FlexibleModel):
    min_workers: int
    max_workers: int

    @root_validator()
    def _validate(cls, values):  # noqa
        assert values["max_workers"] > values["min_workers"], ValueError(
            f"""
        max_workers ({values["max_workers"]}) should be bigger than min_workers ({values["min_workers"]})
        """,
        )
        return values


class AwsAttributes(FlexibleModel):
    first_on_demand: Optional[int]
    availability: Optional[str]
    zone_id: Optional[str]
    instance_profile_arn: Optional[str]
    instance_profile_name: Optional[str]

    @validator("instance_profile_name")
    def _validate(cls, value):  # noqa
        cls.field_deprecated("instance_profile_arn", "instance_profile_name", "instance-profile", value)
        return value


class NewCluster(FlexibleModel):
    spark_version: str
    node_type_id: Optional[str]
    num_workers: Optional[int]
    autoscale: Optional[AutoScale]
    instance_pool_name: Optional[str]
    driver_instance_pool_name: Optional[str]
    driver_instance_pool_id: Optional[str]
    instance_pool_id: Optional[str]
    aws_attributes: Optional[AwsAttributes]
    policy_name: Optional[str]
    policy_id: Optional[str]

    @validator("instance_pool_name")
    def instance_pool_name_validate(cls, value):  # noqa
        cls.field_deprecated("instance_pool_id", "instance_pool_name", "instance-pool", value)
        return value

    @validator("driver_instance_pool_name")
    def driver_instance_pool_name_validate(cls, value):  # noqa
        cls.field_deprecated("driver_instance_pool_id", "driver_instance_pool_name", "instance-pool", value)
        return value

    @validator("policy_name")
    def policy_name_validate(cls, value):  # noqa
        cls.field_deprecated("policy_id", "policy_name", "cluster-policy", value)
        return value
