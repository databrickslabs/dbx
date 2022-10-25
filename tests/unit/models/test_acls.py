import pytest

from dbx.models.workflow.common.access_control import AccessControlMixin


def test_acls_positive():
    acls = AccessControlMixin(
        **{
            "access_control_list": [
                {"user_name": "test1", "permission_level": "IS_OWNER"},
                {"user_name": "test2", "permission_level": "CAN_VIEW"},
            ]
        }
    )
    assert acls.access_control_list is not None


def test_owner_not_provided():
    with pytest.raises(ValueError):
        AccessControlMixin(**{"access_control_list": [{"user_name": "test1", "permission_level": "CAN_MANAGE"}]})


def test_two_owners_provided():
    with pytest.raises(ValueError):
        AccessControlMixin(
            **{
                "access_control_list": [
                    {"user_name": "test1", "permission_level": "IS_OWNER"},
                    {"user_name": "test2", "permission_level": "IS_OWNER"},
                ]
            }
        )


def test_empty_acl():
    _e = AccessControlMixin(**{})
    assert _e.access_control_list is None
