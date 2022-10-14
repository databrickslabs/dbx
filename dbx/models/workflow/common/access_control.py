from enum import Enum
from typing import Optional, List

from dbx.models.exceptions import ValidationError
from dbx.models.validators import at_least_one_of
from dbx.models.workflow._flexible import FlexibleModel
from pydantic import root_validator, validator


class PermissionLevel(str, Enum):
    CAN_MANAGE = "CAN_MANAGE"
    CAN_MANAGE_RUN = "CAN_MANAGE_RUN"
    CAN_VIEW = "CAN_VIEW"
    IS_OWNER = "IS_OWNER"


class AccessControlRequest(FlexibleModel):
    user_name: Optional[str]
    group_name: Optional[str]
    permission_level: PermissionLevel

    _one_of_provided = root_validator(pre=True, allow_reuse=True)(
        lambda _, values: at_least_one_of(["user_name", "group_name"], values)
    )


class AccessControlMixin(FlexibleModel):
    access_control_list: Optional[List[AccessControlRequest]]

    @validator("access_control_list")
    def owner_is_provided(cls, acls: Optional[List[AccessControlRequest]]):  # noqa
        if acls:
            owner_info = [o for o in acls if o.permission_level == PermissionLevel.IS_OWNER]
            if len(owner_info) > 1 or not owner_info:
                raise ValidationError(
                    f"""
                        Workflow should only have one owner, provided: {[o.dict() for o in owner_info]}
                    """
                )
            return acls
