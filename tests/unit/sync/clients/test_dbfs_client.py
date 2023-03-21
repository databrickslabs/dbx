import asyncio
import base64
import textwrap
from tests.unit.sync.utils import create_async_with_result
from unittest.mock import AsyncMock, MagicMock, PropertyMock, call

import pytest

from dbx.sync.clients import ClientError, DBFSClient
from tests.unit.sync.utils import create_async_with_result
from tests.unit.sync.utils import mocked_props, is_dbfs_user_agent


@pytest.fixture
def client(mock_config) -> DBFSClient:
    return DBFSClient(base_path="/tmp/foo", config=mock_config)


def test_init(client):
    assert client.api_token == "fake-token"
    assert client.host == "http://fakehost.asdf"
    assert client.api_base_path == "http://fakehost.asdf/api/2.0/dbfs"


def test_delete(client: DBFSClient):
    session = MagicMock()
    resp = AsyncMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    asyncio.run(client.delete(sub_path="foo/bar", session=session))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/delete"
    assert session.post.call_args[1]["json"] == {"path": "dbfs:/tmp/foo/foo/bar"}
    assert "ssl" not in session.post.call_args[1]
    assert session.post.call_args[1]["headers"]["Authorization"] == "Bearer fake-token"
    assert is_dbfs_user_agent(session.post.call_args[1]["headers"]["user-agent"])


def test_delete_secure(client: DBFSClient):
    mock_config = mocked_props(token="fake-token", host="http://fakehost.asdf/", insecure=False)
    client = DBFSClient(base_path="/tmp/foo", config=mock_config)
    session = MagicMock()
    resp = AsyncMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    asyncio.run(client.delete(sub_path="foo/bar", session=session))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/delete"
    assert session.post.call_args[1]["json"] == {"path": "dbfs:/tmp/foo/foo/bar"}
    assert session.post.call_args[1]["ssl"] is True


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
    resp = AsyncMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    asyncio.run(client.delete(sub_path="foo/bar", session=session, recursive=True))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/delete"
    assert session.post.call_args[1]["json"] == {"path": "dbfs:/tmp/foo/foo/bar", "recursive": True}


def test_delete_rate_limited(client: DBFSClient):
    session = MagicMock()

    rate_limit_resp = MagicMock()
    setattr(type(rate_limit_resp), "status", PropertyMock(return_value=429))

    success_resp = AsyncMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": None}))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.delete(sub_path="foo/bar", session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/delete"
    assert session.post.call_args[1]["json"] == {"path": "dbfs:/tmp/foo/foo/bar"}


def test_delete_rate_limited_retry_after(client: DBFSClient):
    session = MagicMock()

    rate_limit_resp = MagicMock()
    setattr(type(rate_limit_resp), "status", PropertyMock(return_value=429))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": 1}))

    success_resp = AsyncMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.delete(sub_path="foo/bar", session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/delete"
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
    resp = AsyncMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    asyncio.run(client.mkdirs(sub_path="foo/bar", session=session))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/mkdirs"
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

    success_resp = AsyncMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": None}))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.mkdirs(sub_path="foo/bar", session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/mkdirs"
    assert session.post.call_args[1]["json"] == {"path": "dbfs:/tmp/foo/foo/bar"}


def test_mkdirs_rate_limited_retry_after(client: DBFSClient):
    session = MagicMock()

    rate_limit_resp = MagicMock()
    setattr(type(rate_limit_resp), "status", PropertyMock(return_value=429))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": 1}))

    success_resp = AsyncMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.mkdirs(sub_path="foo/bar", session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/mkdirs"
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
    resp = AsyncMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)

    asyncio.run(client.put(sub_path="foo/bar", full_source_path=dummy_file_path, session=session))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/put"
    assert session.post.call_args[1]["json"] == {
        "path": "dbfs:/tmp/foo/foo/bar",
        "contents": base64.b64encode(b"yo").decode("ascii"),
        "overwrite": True,
    }
    assert session.post.call_args[1]["headers"]["Authorization"] == "Bearer fake-token"
    assert is_dbfs_user_agent(session.post.call_args[1]["headers"]["user-agent"])


def test_put_max_block_size_exceeded(client: DBFSClient, dummy_file_path_2mb: str):
    expected_handle = 1234

    async def mock_json(*args, **kwargs):
        return {"handle": expected_handle}

    def mock_post(url, *args, **kwargs):
        resp = AsyncMock()
        setattr(type(resp), "status", PropertyMock(return_value=200))
        if "/api/2.0/dbfs/put" in url:
            contents = kwargs.get("json").get("contents")
            if len(contents) > 1024 * 1024:  # replicate the api error thrown when contents exceeds max allowed
                setattr(type(resp), "status", PropertyMock(return_value=400))
        elif "/api/2.0/dbfs/create" in url:
            # return a mock response json
            resp.json = MagicMock(side_effect=mock_json)

        return create_async_with_result(resp)

    session = AsyncMock()
    post = MagicMock(side_effect=mock_post)
    session.post = post

    asyncio.run(client.put(sub_path="foo/bar", full_source_path=dummy_file_path_2mb, session=session))

    with open(dummy_file_path_2mb, "r") as f:
        expected_contents = f.read()

    chunks = textwrap.wrap(base64.b64encode(bytes(expected_contents, encoding="utf8")).decode("ascii"), 1024 * 1024)

    assert session.post.call_count == len(chunks) + 2
    assert session.post.call_args_list[0][1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/create"
    assert session.post.call_args_list[1][1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/add-block"
    assert session.post.call_args_list[2][1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/add-block"
    assert session.post.call_args_list[3][1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/add-block"
    assert session.post.call_args_list[4][1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/close"

    assert session.post.call_args_list[0][1]["json"] == {
        "path": "dbfs:/tmp/foo/foo/bar",
        "overwrite": True,
    }

    for i, chunk in enumerate(chunks):
        assert session.post.call_args_list[i + 1][1]["json"] == {
            "data": chunk,
            "path": "dbfs:/tmp/foo/foo/bar",
            "handle": expected_handle,
        }, f"invalid json for chunk {i}"

    assert session.post.call_args_list[4][1]["json"] == {
        "path": "dbfs:/tmp/foo/foo/bar",
        "handle": expected_handle,
    }


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

    success_resp = AsyncMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": None}))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.put(sub_path="foo/bar", full_source_path=dummy_file_path, session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/put"
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

    success_resp = AsyncMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.put(sub_path="foo/bar", full_source_path=dummy_file_path, session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/dbfs/put"
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
