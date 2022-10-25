from unittest.mock import MagicMock

import pytest

from dbx.api.adjuster.adjuster import Adjuster, AdditionalLibrariesProvider
from .test_instance_profile import convert_to_workflow

TEST_PAYLOADS = {
    "property": """
    name: "test"
    tasks:
      - task_key: "p1"
        some_task: "a"
    access_control_list:
        - user_name: "service-principal://some-principal"
          permission_level: "IS_OWNER"
    """,
    "duplicated": """
    name: "test"
    tasks:
      - task_key: "p1"
        some_task: "a"
    access_control_list:
        - user_name: "service-principal://some-duplicated-principal"
          permission_level: "IS_OWNER"
    """,
    "not_found": """
    name: "test"
    tasks:
      - task_key: "p1"
        some_task: "a"
    access_control_list:
        - user_name: "service-principal://some-non-existent-principal"
          permission_level: "IS_OWNER"
    """,
}


@pytest.fixture
def service_principal_mock() -> MagicMock:
    client = MagicMock()
    client.perform_query = MagicMock(
        return_value={
            "Resources": [
                {"displayName": "some-principal", "applicationId": "some-id"},
                {"displayName": "some-duplicated-principal", "applicationId": "some-id-1"},
                {"displayName": "some-duplicated-principal", "applicationId": "some-id-2"},
            ]
        }
    )
    return client


@pytest.mark.parametrize("key", list(TEST_PAYLOADS.keys()))
def test_service_principals(key, service_principal_mock):
    _wf = convert_to_workflow(TEST_PAYLOADS[key])
    _adj = Adjuster(
        api_client=service_principal_mock,
        additional_libraries=AdditionalLibrariesProvider(core_package=None),
        file_uploader=MagicMock(),
    )
    if key in ["duplicated", "not_found"]:
        with pytest.raises(AssertionError):
            _adj.traverse(_wf)
    else:
        _adj.traverse(_wf)
        assert _wf[0].access_control_list[0].user_name == "some-id"
