import asyncio
import base64
from tests.unit.sync.utils import create_async_with_result
from unittest.mock import AsyncMock, MagicMock, PropertyMock

import pytest

from dbx.sync.clients import ClientError, DBFSClient
from tests.unit.sync.utils import mocked_props, is_dbfs_user_agent


@pytest.fixture
def client(mock_config) -> DBFSClient:
    return DBFSClient(base_path="/tmp/foo", config=mock_config)


def test_init(client):
    assert client.api_token == "fake-token"
    assert client.host == "http://fakehost.asdf/base"


def test_delete(client: DBFSClient):
    session = MagicMock()
    resp = MagicMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    asyncio.run(client.delete(sub_path="foo/bar", session=session))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/base/api/2.0/dbfs/delete"
    assert session.post.call_args[1]["json"] == {"path": "dbfs:/tmp/foo/foo/bar"}
    assert "ssl" not in session.post.call_args[1]
    assert session.post.call_args[1]["headers"]["Authorization"] == "Bearer fake-token"
    assert is_dbfs_user_agent(session.post.call_args[1]["headers"]["user-agent"])


def test_delete_secure(client: DBFSClient):
    mock_config = mocked_props(token="fake-token", host="http://fakehost.asdf/base/", insecure=False)
    client = DBFSClient(base_path="/tmp/foo", config=mock_config)
    session = MagicMock()
    resp = MagicMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    asyncio.run(client.delete(sub_path="foo/bar", session=session))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/base/api/2.0/dbfs/delete"
    assert session.post.call_args[1]["json"] == {"path": "dbfs:/tmp/foo/foo/bar"}
    assert session.post.call_args[1]["ssl"] is True


def test_delete_secure(client: DBFSClient):
    mock_config = mocked_props(token="fake-token", host="http://fakehost.asdf/base/", insecure=True)
    client = DBFSClient(base_path="/tmp/foo", config=mock_config)
    session = MagicMock()
    resp = MagicMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    asyncio.run(client.delete(sub_path="foo/bar", session=session))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/base/api/2.0/dbfs/delete"
    assert session.post.call_args[1]["json"] == {"path": "dbfs:/tmp/foo/foo/bar"}
    assert session.post.call_args[1]["ssl"] is False


def test_delete_backslash(client: DBFSClient):
    session = MagicMock()
    resp = MagicMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    with pytest.raises(ValueError):
        asyncio.run(client.delete(sub_path="foo\\bar", session=session))


def test_delete_no_path(client: DBFSClient):
    session = MagicMock()
    with pytest.raises(ValueError):
        asyncio.run(client.delete(sub_path=None, session=session))


def test_delete_recursive(client: DBFSClient):
    session = MagicMock()
    resp = MagicMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    asyncio.run(client.delete(sub_path="foo/bar", session=session, recursive=True))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/base/api/2.0/dbfs/delete"
    assert session.post.call_args[1]["json"] == {"path": "dbfs:/tmp/foo/foo/bar", "recursive": True}


def test_delete_rate_limited(client: DBFSClient):
    session = MagicMock()

    rate_limit_resp = MagicMock()
    setattr(type(rate_limit_resp), "status", PropertyMock(return_value=429))

    success_resp = MagicMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": None}))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.delete(sub_path="foo/bar", session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/base/api/2.0/dbfs/delete"
    assert session.post.call_args[1]["json"] == {"path": "dbfs:/tmp/foo/foo/bar"}


def test_delete_rate_limited_retry_after(client: DBFSClient):
    session = MagicMock()

    rate_limit_resp = MagicMock()
    setattr(type(rate_limit_resp), "status", PropertyMock(return_value=429))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": 1}))

    success_resp = MagicMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.delete(sub_path="foo/bar", session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/base/api/2.0/dbfs/delete"
    assert session.post.call_args[1]["json"] == {"path": "dbfs:/tmp/foo/foo/bar"}


def test_delete_unauthorized(client: DBFSClient):
    session = MagicMock()

    unauth_resp = AsyncMock()
    setattr(type(unauth_resp), "status", PropertyMock(return_value=401))

    session.post.return_value = unauth_resp

    unauth_resp.text.return_value = "bad auth"

    with pytest.raises(ClientError):
        asyncio.run(client.delete(sub_path="foo/bar", session=session))


def test_mkdirs(client: DBFSClient):
    session = MagicMock()
    resp = MagicMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    asyncio.run(client.mkdirs(sub_path="foo/bar", session=session))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/base/api/2.0/dbfs/mkdirs"
    assert session.post.call_args[1]["json"] == {"path": "dbfs:/tmp/foo/foo/bar"}
    assert session.post.call_args[1]["headers"]["Authorization"] == "Bearer fake-token"
    assert is_dbfs_user_agent(session.post.call_args[1]["headers"]["user-agent"])


def test_mkdirs_backslash(client: DBFSClient):
    session = MagicMock()
    resp = MagicMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    with pytest.raises(ValueError):
        asyncio.run(client.mkdirs(sub_path="foo\\bar", session=session))


def test_mkdirs_no_path(client: DBFSClient):
    session = MagicMock()
    with pytest.raises(ValueError):
        asyncio.run(client.mkdirs(sub_path=None, session=session))


def test_mkdirs_rate_limited(client: DBFSClient):
    session = MagicMock()

    rate_limit_resp = MagicMock()
    setattr(type(rate_limit_resp), "status", PropertyMock(return_value=429))

    success_resp = MagicMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": None}))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.mkdirs(sub_path="foo/bar", session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/base/api/2.0/dbfs/mkdirs"
    assert session.post.call_args[1]["json"] == {"path": "dbfs:/tmp/foo/foo/bar"}


def test_mkdirs_rate_limited_retry_after(client: DBFSClient):
    session = MagicMock()

    rate_limit_resp = MagicMock()
    setattr(type(rate_limit_resp), "status", PropertyMock(return_value=429))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": 1}))

    success_resp = MagicMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.mkdirs(sub_path="foo/bar", session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/base/api/2.0/dbfs/mkdirs"
    assert session.post.call_args[1]["json"] == {"path": "dbfs:/tmp/foo/foo/bar"}


def test_mkdirs_unauthorized(client: DBFSClient):
    session = MagicMock()

    unauth_resp = AsyncMock()
    setattr(type(unauth_resp), "status", PropertyMock(return_value=401))

    session.post.return_value = unauth_resp

    unauth_resp.text.return_value = "bad auth"

    with pytest.raises(ClientError):
        asyncio.run(client.mkdirs(sub_path="foo/bar", session=session))


def test_put(client: DBFSClient, dummy_file_path: str):
    session = MagicMock()
    resp = MagicMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)

    asyncio.run(client.put(sub_path="foo/bar", full_source_path=dummy_file_path, session=session))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/base/api/2.0/dbfs/put"
    assert session.post.call_args[1]["json"] == {
        "path": "dbfs:/tmp/foo/foo/bar",
        "contents": base64.b64encode(b"yo").decode("ascii"),
        "overwrite": True,
    }
    assert session.post.call_args[1]["headers"]["Authorization"] == "Bearer fake-token"
    assert is_dbfs_user_agent(session.post.call_args[1]["headers"]["user-agent"])


def test_put_backslash(client: DBFSClient, dummy_file_path: str):
    session = MagicMock()
    resp = MagicMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)

    with pytest.raises(ValueError):
        asyncio.run(client.put(sub_path="foo\\bar", full_source_path=dummy_file_path, session=session))


def test_put_no_path(client: DBFSClient, dummy_file_path: str):
    session = MagicMock()

    with pytest.raises(ValueError):
        asyncio.run(client.put(sub_path=None, full_source_path=dummy_file_path, session=session))


def test_put_rate_limited(client: DBFSClient, dummy_file_path: str):
    session = MagicMock()

    rate_limit_resp = MagicMock()
    setattr(type(rate_limit_resp), "status", PropertyMock(return_value=429))

    success_resp = MagicMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": None}))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.put(sub_path="foo/bar", full_source_path=dummy_file_path, session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/base/api/2.0/dbfs/put"
    assert session.post.call_args[1]["json"] == {
        "path": "dbfs:/tmp/foo/foo/bar",
        "contents": base64.b64encode(b"yo").decode("ascii"),
        "overwrite": True,
    }


def test_put_rate_limited_retry_after(client: DBFSClient, dummy_file_path: str):
    session = MagicMock()

    rate_limit_resp = MagicMock()
    setattr(type(rate_limit_resp), "status", PropertyMock(return_value=429))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": 1}))

    success_resp = MagicMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.put(sub_path="foo/bar", full_source_path=dummy_file_path, session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/base/api/2.0/dbfs/put"
    assert session.post.call_args[1]["json"] == {
        "path": "dbfs:/tmp/foo/foo/bar",
        "contents": base64.b64encode(b"yo").decode("ascii"),
        "overwrite": True,
    }


def test_put_unauthorized(client: DBFSClient, dummy_file_path: str):
    session = MagicMock()

    unauth_resp = AsyncMock()
    setattr(type(unauth_resp), "status", PropertyMock(return_value=401))

    session.post.return_value = unauth_resp

    unauth_resp.text.return_value = "bad auth"

    with pytest.raises(ClientError):
        asyncio.run(client.put(sub_path="foo/bar", full_source_path=dummy_file_path, session=session))
