import asyncio
from unittest.mock import AsyncMock, MagicMock, PropertyMock

import pytest

from dbx.sync.clients import ClientError, ReposClient
from tests.unit.sync.utils import create_async_with_result
from tests.unit.sync.utils import mocked_props, is_repos_user_agent


@pytest.fixture
def client(mock_config):
    return ReposClient(user="foo@somewhere.com", repo_name="my-repo", config=mock_config)


def test_init(mock_config):
    client = ReposClient(user="foo@somewhere.com", repo_name="my-repo", config=mock_config)
    assert client.api_token == "fake-token"
    assert client.host == "http://fakehost.asdf"
    assert client.workspace_api_base_path == "http://fakehost.asdf/api/2.0/workspace"
    assert client.workspace_files_api_base_path == "http://fakehost.asdf/api/2.0/workspace-files/import-file"
    assert client.base_path == "/Repos/foo@somewhere.com/my-repo"

    with pytest.raises(ValueError):
        ReposClient(user="", repo_name="my-repo", config=mock_config)
    with pytest.raises(ValueError):
        ReposClient(user=None, repo_name="my-repo", config=mock_config)
    with pytest.raises(ValueError):
        ReposClient(user="foo@somewhere.com", repo_name="", config=mock_config)
    with pytest.raises(ValueError):
        ReposClient(user="foo@somewhere.com", repo_name=None, config=mock_config)


def test_delete(client: ReposClient):
    session = MagicMock()
    resp = AsyncMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    asyncio.run(client.delete(sub_path="foo/bar", session=session))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/workspace/delete"
    assert session.post.call_args[1]["json"] == {"path": "/Repos/foo@somewhere.com/my-repo/foo/bar"}
    assert "ssl" not in session.post.call_args[1]
    assert session.post.call_args[1]["headers"]["Authorization"] == "Bearer fake-token"
    assert is_repos_user_agent(session.post.call_args[1]["headers"]["user-agent"])


def test_delete_secure(client: ReposClient):
    mock_config = mocked_props(token="fake-token", host="http://fakehost.asdf/", insecure=False)
    client = ReposClient(user="foo@somewhere.com", repo_name="my-repo", config=mock_config)
    session = MagicMock()
    resp = AsyncMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    asyncio.run(client.delete(sub_path="foo/bar", session=session))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/workspace/delete"
    assert session.post.call_args[1]["json"] == {"path": "/Repos/foo@somewhere.com/my-repo/foo/bar"}
    assert session.post.call_args[1]["ssl"] is True


def test_delete_insecure(client: ReposClient):
    mock_config = mocked_props(token="fake-token", host="http://fakehost.asdf/", insecure=True)
    client = ReposClient(user="foo@somewhere.com", repo_name="my-repo", config=mock_config)
    session = MagicMock()
    resp = AsyncMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    asyncio.run(client.delete(sub_path="foo/bar", session=session))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/workspace/delete"
    assert session.post.call_args[1]["json"] == {"path": "/Repos/foo@somewhere.com/my-repo/foo/bar"}
    assert session.post.call_args[1]["ssl"] is False


def test_delete_backslash(client: ReposClient):
    session = MagicMock()
    resp = MagicMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    with pytest.raises(ValueError):
        asyncio.run(client.delete(sub_path="foo\\bar", session=session))


def test_delete_no_path(client: ReposClient):
    session = MagicMock()
    with pytest.raises(ValueError):
        asyncio.run(client.delete(sub_path=None, session=session))


def test_delete_recursive(client: ReposClient):
    session = MagicMock()
    resp = AsyncMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    asyncio.run(client.delete(sub_path="foo/bar", session=session, recursive=True))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/workspace/delete"
    assert session.post.call_args[1]["json"] == {"path": "/Repos/foo@somewhere.com/my-repo/foo/bar", "recursive": True}


def test_delete_rate_limited(client: ReposClient):
    session = MagicMock()

    rate_limit_resp = MagicMock()
    setattr(type(rate_limit_resp), "status", PropertyMock(return_value=429))

    success_resp = AsyncMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": None}))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.delete(sub_path="foo/bar", session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/workspace/delete"
    assert session.post.call_args[1]["json"] == {"path": "/Repos/foo@somewhere.com/my-repo/foo/bar"}


def test_delete_rate_limited_retry_after(client: ReposClient):
    session = MagicMock()

    rate_limit_resp = MagicMock()
    setattr(type(rate_limit_resp), "status", PropertyMock(return_value=429))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": 1}))

    success_resp = AsyncMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.delete(sub_path="foo/bar", session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/workspace/delete"
    assert session.post.call_args[1]["json"] == {"path": "/Repos/foo@somewhere.com/my-repo/foo/bar"}


def test_delete_unauthorized(client: ReposClient):
    session = MagicMock()

    unauth_resp = AsyncMock()
    setattr(type(unauth_resp), "status", PropertyMock(return_value=401))

    session.post.return_value = unauth_resp

    unauth_resp.text.return_value = "bad auth"

    with pytest.raises(ClientError):
        asyncio.run(client.delete(sub_path="foo/bar", session=session))


def test_mkdirs(client: ReposClient):
    session = MagicMock()
    resp = AsyncMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    asyncio.run(client.mkdirs(sub_path="foo/bar", session=session))

    assert session.post.call_count == 1
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/workspace/mkdirs"
    assert session.post.call_args[1]["json"] == {"path": "/Repos/foo@somewhere.com/my-repo/foo/bar"}
    assert session.post.call_args[1]["headers"]["Authorization"] == "Bearer fake-token"
    assert is_repos_user_agent(session.post.call_args[1]["headers"]["user-agent"])


def test_mkdirs_backslash(client: ReposClient):
    session = MagicMock()
    resp = MagicMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)
    with pytest.raises(ValueError):
        asyncio.run(client.mkdirs(sub_path="foo\\bar", session=session))


def test_mkdirs_no_path(client: ReposClient):
    session = MagicMock()
    with pytest.raises(ValueError):
        asyncio.run(client.mkdirs(sub_path=None, session=session))


def test_mkdirs_rate_limited(client: ReposClient):
    session = MagicMock()

    rate_limit_resp = MagicMock()
    setattr(type(rate_limit_resp), "status", PropertyMock(return_value=429))

    success_resp = AsyncMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": None}))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.mkdirs(sub_path="foo/bar", session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/workspace/mkdirs"
    assert session.post.call_args[1]["json"] == {"path": "/Repos/foo@somewhere.com/my-repo/foo/bar"}


def test_mkdirs_rate_limited_retry_after(client: ReposClient):
    session = MagicMock()

    rate_limit_resp = MagicMock()
    setattr(type(rate_limit_resp), "status", PropertyMock(return_value=429))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": 1}))

    success_resp = AsyncMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.mkdirs(sub_path="foo/bar", session=session))

    assert session.post.call_count == 2
    assert session.post.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/workspace/mkdirs"
    assert session.post.call_args[1]["json"] == {"path": "/Repos/foo@somewhere.com/my-repo/foo/bar"}


def test_mkdirs_unauthorized(client: ReposClient):
    session = MagicMock()

    unauth_resp = AsyncMock()
    setattr(type(unauth_resp), "status", PropertyMock(return_value=401))

    session.post.return_value = unauth_resp

    unauth_resp.text.return_value = "bad auth"

    with pytest.raises(ClientError):
        asyncio.run(client.mkdirs(sub_path="foo/bar", session=session))


def test_put(client: ReposClient, dummy_file_path: str):
    session = MagicMock()
    resp = MagicMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)

    asyncio.run(client.put(sub_path="foo/bar", full_source_path=dummy_file_path, session=session))

    assert session.post.call_count == 1
    assert (
        session.post.call_args[1]["url"]
        == "http://fakehost.asdf/api/2.0/workspace-files/import-file/Repos/foo@somewhere.com/my-repo/foo/bar"
    )
    assert session.post.call_args[1]["data"] == b"yo"
    assert session.post.call_args[1]["headers"]["Authorization"] == "Bearer fake-token"
    assert is_repos_user_agent(session.post.call_args[1]["headers"]["user-agent"])


def test_put_backslash(client: ReposClient, dummy_file_path: str):
    session = MagicMock()
    resp = MagicMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    session.post.return_value = create_async_with_result(resp)

    with pytest.raises(ValueError):
        asyncio.run(client.put(sub_path="foo\\bar", full_source_path=dummy_file_path, session=session))


def test_put_no_path(client: ReposClient, dummy_file_path: str):
    session = MagicMock()

    with pytest.raises(ValueError):
        asyncio.run(client.put(sub_path=None, full_source_path=dummy_file_path, session=session))


def test_put_rate_limited(client: ReposClient, dummy_file_path: str):
    session = MagicMock()

    rate_limit_resp = MagicMock()
    setattr(type(rate_limit_resp), "status", PropertyMock(return_value=429))

    success_resp = MagicMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": None}))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.put(sub_path="foo/bar", full_source_path=dummy_file_path, session=session))

    assert session.post.call_count == 2
    assert (
        session.post.call_args[1]["url"]
        == "http://fakehost.asdf/api/2.0/workspace-files/import-file/Repos/foo@somewhere.com/my-repo/foo/bar"
    )
    assert session.post.call_args[1]["data"] == b"yo"


def test_put_rate_limited_retry_after(client: ReposClient, dummy_file_path: str):
    session = MagicMock()

    rate_limit_resp = MagicMock()
    setattr(type(rate_limit_resp), "status", PropertyMock(return_value=429))
    setattr(type(rate_limit_resp), "headers", PropertyMock(return_value={"Retry-After": 1}))

    success_resp = MagicMock()
    setattr(type(success_resp), "status", PropertyMock(return_value=200))

    session.post.side_effect = [create_async_with_result(rate_limit_resp), create_async_with_result(success_resp)]

    asyncio.run(client.put(sub_path="foo/bar", full_source_path=dummy_file_path, session=session))

    assert session.post.call_count == 2
    assert (
        session.post.call_args[1]["url"]
        == "http://fakehost.asdf/api/2.0/workspace-files/import-file/Repos/foo@somewhere.com/my-repo/foo/bar"
    )
    assert session.post.call_args[1]["data"] == b"yo"


def test_put_unauthorized(client: ReposClient, dummy_file_path: str):
    session = MagicMock()

    unauth_resp = AsyncMock()
    setattr(type(unauth_resp), "status", PropertyMock(return_value=401))

    session.post.return_value = unauth_resp

    unauth_resp.text.return_value = "bad auth"

    with pytest.raises(ClientError):
        asyncio.run(client.put(sub_path="foo/bar", full_source_path=dummy_file_path, session=session))


def test_exists(client: ReposClient):
    session = MagicMock()
    resp = AsyncMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    resp.json.return_value = {"repos": [{"path": "/Repos/foo@somewhere.com/my-repo"}]}
    session.get.return_value = create_async_with_result(resp)
    assert asyncio.run(client.exists(session=session))

    assert session.get.call_count == 1
    assert session.get.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/repos"
    assert session.get.call_args[1]["params"] == {"path_prefix": "/Repos/foo@somewhere.com/my-repo"}
    assert "ssl" not in session.get.call_args[1]
    assert session.get.call_args[1]["headers"]["Authorization"] == "Bearer fake-token"
    assert is_repos_user_agent(session.get.call_args[1]["headers"]["user-agent"])


def test_exists_not_found(client: ReposClient):
    session = MagicMock()
    resp = AsyncMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    resp.json.return_value = {"repos": [{"path": "/Repos/foo@somewhere.com/other-repo"}]}
    session.get.return_value = create_async_with_result(resp)
    assert not asyncio.run(client.exists(session=session))

    assert session.get.call_count == 1
    assert session.get.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/repos"
    assert session.get.call_args[1]["params"] == {"path_prefix": "/Repos/foo@somewhere.com/my-repo"}
    assert "ssl" not in session.get.call_args[1]
    assert session.get.call_args[1]["headers"]["Authorization"] == "Bearer fake-token"
    assert is_repos_user_agent(session.get.call_args[1]["headers"]["user-agent"])


def test_exists_empty_response(client: ReposClient):
    session = MagicMock()
    resp = AsyncMock()
    setattr(type(resp), "status", PropertyMock(return_value=200))
    resp.json.return_value = {}
    session.get.return_value = create_async_with_result(resp)
    assert not asyncio.run(client.exists(session=session))

    assert session.get.call_count == 1
    assert session.get.call_args[1]["url"] == "http://fakehost.asdf/api/2.0/repos"
    assert session.get.call_args[1]["params"] == {"path_prefix": "/Repos/foo@somewhere.com/my-repo"}
    assert "ssl" not in session.get.call_args[1]
    assert session.get.call_args[1]["headers"]["Authorization"] == "Bearer fake-token"
    assert is_repos_user_agent(session.get.call_args[1]["headers"]["user-agent"])


def test_exists_failure(client: ReposClient):
    session = MagicMock()
    resp = AsyncMock()
    setattr(type(resp), "status", PropertyMock(return_value=500))
    resp.json.return_value = {"repos": [{"path": "/Repos/foo@somewhere.com/my-repo"}]}
    session.get.return_value = create_async_with_result(resp)
    with pytest.raises(ClientError):
        asyncio.run(client.exists(session=session))
