import asyncio
import base64
from abc import ABC, abstractmethod

import aiohttp
import requests
from databricks_cli.configure.provider import DatabricksConfig

from dbx.utils import dbx_echo


class ClientError(Exception):
    pass


class BaseClient(ABC):
    @abstractmethod
    async def delete(self, sub_path: str, *, session: aiohttp.ClientSession, recursive: bool = False):
        raise NotImplementedError

    @abstractmethod
    async def mkdirs(self, sub_path: str, *, session: aiohttp.ClientSession):
        raise NotImplementedError

    @abstractmethod
    async def put(self, sub_path: str, full_source_path: str, *, session: aiohttp.ClientSession):
        raise NotImplementedError


def get_auth_headers(api_token: str) -> dict:
    headers = {"Authorization": f"Bearer {api_token}"}
    return headers


def get_user(config: DatabricksConfig) -> dict:
    """Gets information about the user associated with the token in the config.

    Args:
        config (DatabricksConfig): config which contains the API token

    Returns:
        dict: information about the user, such as their userName, or None if the API returns an error
              or isn't supported
    """
    api_token = config.token
    host = config.host.rstrip("/")
    headers = get_auth_headers(api_token)
    url = f"{host}/api/2.0/preview/scim/v2/Me"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json()
    return None


async def _rate_limit_sleep(resp, *, default_sleep=0.5):
    """
    Sleep in response to a rate limit (429) error.
    """

    retry_after = default_sleep
    if resp.headers.get("Retry-After"):
        retry_after = int(resp.headers["Retry-After"])
    dbx_echo(f"Sleeping {retry_after:.1f} seconds")
    await asyncio.sleep(retry_after)


async def _api(
    *, url: str, path: str, session: aiohttp.ClientSession, api_token: str,
    ok_status={200}, **more_json_data
):
    json_data = {"path": path, **more_json_data}
    while True:
        headers = get_auth_headers(api_token)
        async with session.post(url=url, json=json_data, headers=headers) as resp:
            if resp.status in ok_status:
                break
            if resp.status == 429:
                dbx_echo("Rate limited")
                await _rate_limit_sleep(resp)
            else:
                txt = await resp.text()
                dbx_echo(f"HTTP {resp.status}: {txt}")
                raise ClientError(resp.status)


async def _api_delete(
    *, api_base_path: str, path: str, session: aiohttp.ClientSession, recursive: bool = False, api_token: str
):
    dbx_echo(f"Deleting {path}")
    more_opts = {"recursive": True} if recursive else {}
    await _api(url=f"{api_base_path}/delete", path=path, session=session, api_token=api_token, ok_status={200, 404},
               **more_opts)


async def _api_mkdirs(*, api_base_path: str, path: str, session: aiohttp.ClientSession, api_token: str):
    dbx_echo(f"Creating {path}")
    await _api(url=f"{api_base_path}/mkdirs", path=path, session=session, api_token=api_token)


async def _api_put(*, api_base_path: str, path: str, session: aiohttp.ClientSession, api_token: str, **data):
    dbx_echo(f"Putting {path}")
    more_opts = {"overwrite": True, **data}
    await _api(url=f"{api_base_path}/put", path=path, session=session, api_token=api_token, **more_opts)


def check_path(path: str) -> None:
    if not path:
        raise ValueError("Path is empty")
    if "\\" in path:
        raise ValueError("Paths should not contain backslashes")


class DBFSClient(BaseClient):
    name = "dbfs"

    def __init__(self, *, base_path: str, config: DatabricksConfig):
        check_path(base_path)
        self.base_path = "dbfs:" + base_path.rstrip("/")
        self.api_token = config.token
        self.host = config.host.rstrip("/")
        self.api_base_path = f"{self.host}/api/2.0/dbfs"

        dbx_echo(f"Target base path: {self.base_path}")

    async def delete(self, sub_path: str, *, session: aiohttp.ClientSession, recursive: bool = False):
        check_path(sub_path)
        path = f"{self.base_path}/{sub_path}"
        await _api_delete(
            api_base_path=self.api_base_path, path=path, session=session, recursive=recursive, api_token=self.api_token
        )

    async def mkdirs(self, sub_path: str, *, session: aiohttp.ClientSession):
        check_path(sub_path)
        path = f"{self.base_path}/{sub_path}"
        await _api_mkdirs(api_base_path=self.api_base_path, path=path, session=session, api_token=self.api_token)

    async def put(
        self,
        sub_path: str,
        full_source_path: str,
        *,
        session: aiohttp.ClientSession,
    ):
        check_path(sub_path)
        path = f"{self.base_path}/{sub_path}"
        with open(full_source_path, "rb") as f:
            contents = base64.b64encode(f.read()).decode("ascii")
        await _api_put(api_base_path=self.api_base_path, path=path, session=session, api_token=self.api_token,
                       contents=contents)


class ReposClient(BaseClient):
    name = "repos"

    def __init__(self, *, user: str, repo_name: str, config: DatabricksConfig):
        if not user:
            raise ValueError("Expected a user")
        if not repo_name:
            raise ValueError("repo_name is required")
        self.base_path = f"/Repos/{user}/{repo_name}"
        self.api_token = config.token
        self.host = config.host.rstrip("/")
        self.workspace_api_base_path = f"{self.host}/api/2.0/workspace"
        self.workspace_files_api_base_path = f"{self.host}/api/2.0/workspace-files/import-file"

        dbx_echo(f"Target base path: {self.base_path}")

    async def delete(self, sub_path: str, *, session: aiohttp.ClientSession, recursive: bool = False):
        check_path(sub_path)
        path = f"{self.base_path}/{sub_path}"
        await _api_delete(
            api_base_path=self.workspace_api_base_path,
            path=path,
            session=session,
            recursive=recursive,
            api_token=self.api_token,
        )

    async def mkdirs(self, sub_path: str, *, session: aiohttp.ClientSession):
        check_path(sub_path)
        path = f"{self.base_path}/{sub_path}"
        await _api_mkdirs(
            api_base_path=self.workspace_api_base_path, path=path, session=session, api_token=self.api_token
        )

    async def put(self, sub_path: str, full_source_path: str, *, session: aiohttp.ClientSession):
        check_path(sub_path)
        path = f"{self.base_path}/{sub_path}"
        dbx_echo(f"Putting {path}")
        with open(full_source_path, "rb") as f:
            content = f.read()
            path = path.lstrip("/")
            url = f"{self.workspace_files_api_base_path}/{path}"
            params = {"overwrite": "true"}
            while True:
                headers = get_auth_headers(self.api_token)
                async with session.post(url=url, data=content, params=params, headers=headers) as resp:
                    if resp.status == 200:
                        break
                    if resp.status == 429:
                        dbx_echo("Rate limited")
                        await _rate_limit_sleep(resp)
                    else:
                        txt = await resp.text()
                        dbx_echo(f"HTTP {resp.status}: {txt}")
                        raise ClientError(resp.status)
