from typing import Optional

from dbx.models.base import FlexibleBaseModel


class AwsAttributes(FlexibleBaseModel):
    instance_profile_arn: Optional[str]
    # derivable field used in the named properties conversion
    instance_profile_name: Optional[str]


class NewCluster(FlexibleBaseModel):
    driver_instance_pool_id: Optional[str]
    instance_pool_id: Optional[str]
    policy_id: Optional[str]
    aws_attributes: Optional[AwsAttributes]
    # derivable fields used in the named properties conversion and policy propagation features
    driver_instance_pool_name: Optional[str]
    instance_pool_name: Optional[str]
    policy_name: Optional[str]


class JobCluster(FlexibleBaseModel):
    job_cluster_key: str
    new_cluster: NewCluster
