from unittest.mock import MagicMock, PropertyMock, patch

from dbx.sync.clients import get_user


@patch("dbx.sync.clients.requests")
def test_get_user(mock_requests, mock_config):
    resp = MagicMock()
    setattr(type(resp), "status_code", PropertyMock(return_value=200))
    user_info = {"userName": "foo"}
    resp.json.return_value = user_info
    mock_requests.get.return_value = resp
    assert get_user(mock_config) == user_info
    assert resp.json.call_count == 1


@patch("dbx.sync.clients.requests")
def test_get_user_failed(mock_requests, mock_config):
    resp = MagicMock()
    setattr(type(resp), "status_code", PropertyMock(return_value=404))
    resp.json.return_value = {}
    mock_requests.get.return_value = resp
    assert get_user(mock_config) is None
    assert resp.json.call_count == 0
