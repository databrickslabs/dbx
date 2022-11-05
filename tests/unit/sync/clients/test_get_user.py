from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from dbx.sync.clients import get_user
from tests.unit.sync.utils import mocked_props


@pytest.mark.parametrize(
    "test_case",
    [
        ("http://fakehost.asdf", "http://fakehost.asdf/api/2.0/preview/scim/v2/Me"),
        ("http://fakehost.asdf/", "http://fakehost.asdf/api/2.0/preview/scim/v2/Me"),
        ("http://fakehost.asdf/?o=1234", "http://fakehost.asdf/api/2.0/preview/scim/v2/Me"),
        ("http://fakehost.asdf:8080/?o=1234", "http://fakehost.asdf:8080/api/2.0/preview/scim/v2/Me"),
    ],
)
@patch("dbx.sync.clients.requests")
def test_get_user(mock_requests, test_case):
    config_host, expected_url = test_case
    mock_config = mocked_props(token="fake-token", host=config_host, insecure=None)
    resp = MagicMock()
    setattr(type(resp), "status_code", PropertyMock(return_value=200))
    user_info = {"userName": "foo"}
    resp.json.return_value = user_info
    mock_requests.get.return_value = resp
    assert get_user(mock_config) == user_info
    assert resp.json.call_count == 1
    assert mock_requests.get.call_args[0][0] == expected_url


@patch("dbx.sync.clients.requests")
def test_get_user_failed(mock_requests, mock_config):
    resp = MagicMock()
    setattr(type(resp), "status_code", PropertyMock(return_value=404))
    resp.json.return_value = {}
    mock_requests.get.return_value = resp
    assert get_user(mock_config) is None
    assert resp.json.call_count == 0
