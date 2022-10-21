from enum import Enum
from typing import Optional, List, Dict, Any

from pydantic import root_validator, validator

from dbx.models.validators import at_least_one_of
from dbx.models.workflow.common.flexible import FlexibleModel


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
    def owner_is_provided(cls, acls: List[AccessControlRequest]):  # noqa
        owner_info = [o for o in acls if o.permission_level == PermissionLevel.IS_OWNER]
        if not owner_info:
            raise ValueError("At least one owner (IS_OWNER) should be provided in the access control list")
        if len(owner_info) > 1:
            raise ValueError("Only one owner should be provided in the access control list")
        return acls

    def get_acl_payload(self) -> Dict[str, Any]:
        return self.dict(exclude_none=True)
