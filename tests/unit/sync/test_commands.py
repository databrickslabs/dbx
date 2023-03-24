import asyncio
import os
from unittest.mock import patch, call, MagicMock, AsyncMock

import click
import pytest
from databricks_cli.configure.provider import ProfileConfigProvider

from dbx.commands.sync.sync import repo_exists
from dbx.commands.sync.functions import get_user_name, get_source_base_name
from dbx.constants import DBX_SYNC_DEFAULT_IGNORES
from dbx.sync import DeleteUnmatchedOption
from dbx.sync.clients import DBFSClient, ReposClient, WorkspaceClient
from tests.unit.sync.utils import mocked_props
from .conftest import invoke_cli_runner
from .utils import temporary_directory, pushd


def get_config():
    return mocked_props(token="fake-token", host="http://fakehost.asdf/?o=1234", insecure=None)


@pytest.fixture
def mock_get_config():
    with patch("dbx.commands.sync.sync.get_databricks_config") as mock_get_databricks_config:
        config = get_config()
        mock_get_databricks_config.return_value = config
        yield mock_get_databricks_config


@patch("dbx.commands.sync.functions.get_user")
def test_get_user_name(mock_get_user):
    mock_get_user.return_value = {"userName": "foo"}
    config = MagicMock()
    assert get_user_name(config) == "foo"
    assert mock_get_user.call_count == 1
    assert mock_get_user.call_args == call(config)


def test_get_source_base_name():
    assert get_source_base_name("/foo") == "foo"
    assert get_source_base_name("/foo/bar") == "bar"
    assert get_source_base_name("/foo/bar/") == "bar"
    with pytest.raises(click.UsageError):
        get_source_base_name("/")


@patch("dbx.commands.sync.sync.main_loop")
def test_repo_no_opts(mock_main_loop):
    # some options are required
    res = invoke_cli_runner(["repo"], expected_error=True)
    assert "Missing option" in res.output


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_environment(mock_main_loop, mock_get_user_name, mock_repo_exists, temp_project):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True
        mock_get_user_name.return_value = "me"

        with patch.object(ProfileConfigProvider, "get_config") as config_mock:
            config_mock.return_value = get_config()
            invoke_cli_runner(["repo", "-s", tempdir, "-d", "the-repo", "--environment", "default"])

            assert mock_main_loop.call_count == 1
            assert config_mock.call_count == 1
            assert mock_get_user_name.call_count == 1


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_dbfs_environment(mock_main_loop, mock_get_user_name, temp_project):
    with temporary_directory() as tempdir:
        mock_get_user_name.return_value = "me"

        with patch.object(ProfileConfigProvider, "get_config") as config_mock:
            config_mock.return_value = get_config()
            invoke_cli_runner(["dbfs", "-s", tempdir, "-d", "the-repo", "--environment", "default"])

            assert mock_main_loop.call_count == 1
            assert config_mock.call_count == 1


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_basic_opts(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True
        mock_get_user_name.return_value = "me"

        invoke_cli_runner(["repo", "-s", tempdir, "-d", "the-repo"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_unknown_user(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True
        mock_get_user_name.return_value = None

        res = invoke_cli_runner(["repo", "-s", tempdir, "-d", "the-repo"], expected_error=True)

        assert mock_main_loop.call_count == 0
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 1

        assert "Destination repo path can't be automatically determined because the user is" in res.output


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_unknown_repo(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = False
        mock_get_user_name.return_value = "me"

        res = invoke_cli_runner(["repo", "-s", tempdir, "-d", "the-repo"], expected_error=True)

        assert mock_main_loop.call_count == 0
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 1

        assert "lease create the repo" in res.output


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_dry_run(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True

        invoke_cli_runner(["repo", "-s", tempdir, "-d", "the-repo", "-u", "me", "--dry-run"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert not mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_polling(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True
        mock_get_user_name.return_value = "me"

        invoke_cli_runner(["repo", "-s", tempdir, "-d", "the-repo", "--polling-interval", "2"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["polling_interval_secs"] == 2.0
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_include_dir(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True

        os.mkdir(os.path.join(tempdir, "foo"))

        invoke_cli_runner(["repo", "-s", tempdir, "-d", "the-repo", "-u", "me", "-i", "foo"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == ["/foo/"]
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_force_include_dir(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True

        os.mkdir(os.path.join(tempdir, "foo"))

        invoke_cli_runner(["repo", "-s", tempdir, "-d", "the-repo", "-u", "me", "-fi", "foo"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == ["/foo/"]
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_include_pattern(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True

        os.mkdir(os.path.join(tempdir, "foo"))

        invoke_cli_runner(["repo", "-s", tempdir, "-d", "the-repo", "-u", "me", "-ip", "foo/*.py"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == ["foo/*.py"]
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_force_include_pattern(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True

        os.mkdir(os.path.join(tempdir, "foo"))

        invoke_cli_runner(["repo", "-s", tempdir, "-d", "the-repo", "-u", "me", "-fip", "foo/*.py"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == ["foo/*.py"]
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_exclude_dir(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True

        os.mkdir(os.path.join(tempdir, "foo"))

        invoke_cli_runner(["repo", "-s", tempdir, "-d", "the-repo", "-u", "me", "-e", "foo"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert sorted(mock_main_loop.call_args[1]["matcher"].ignores) == sorted(DBX_SYNC_DEFAULT_IGNORES + ["/foo/"])
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_exclude_pattern(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True

        os.mkdir(os.path.join(tempdir, "foo"))

        invoke_cli_runner(["repo", "-s", tempdir, "-d", "the-repo", "-u", "me", "-ep", "foo/**/*.py"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert sorted(mock_main_loop.call_args[1]["matcher"].ignores) == sorted(
            DBX_SYNC_DEFAULT_IGNORES + ["foo/**/*.py"]
        )
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_include_dir_not_exists(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True

        # we don't create the "foo" subdir, so it should produce an error

        res = invoke_cli_runner(["repo", "-s", tempdir, "-d", "the-repo", "-u", "me", "-i", "foo"], expected_error=True)

        assert mock_main_loop.call_count == 0
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert "does not exist" in res.output


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_inferred_source(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir, pushd(tempdir):
        mock_repo_exists.return_value = True

        os.mkdir(os.path.join(tempdir, ".git"))

        invoke_cli_runner(["repo", "-d", "the-repo", "-u", "me"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_inferred_source_no_git(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir, pushd(tempdir):
        mock_repo_exists.return_value = True

        # source can only be inferred when the cwd contains a .git subdir

        res = invoke_cli_runner(["repo", "-d", "the-repo", "-u", "me"], expected_error=True)

        assert mock_main_loop.call_count == 0
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert "Must specify source" in res.output


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_allow_delete_unmatched(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True

        invoke_cli_runner(
            ["repo", "-s", tempdir, "-d", "the-repo", "-u", "me", "--unmatched-behaviour=allow-delete-unmatched"]
        )

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.ALLOW_DELETE_UNMATCHED

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_disallow_delete_unmatched(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True

        invoke_cli_runner(
            ["repo", "-s", tempdir, "-d", "the-repo", "-u", "me", "--unmatched-behaviour=disallow-delete-unmatched"]
        )

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.DISALLOW_DELETE_UNMATCHED

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_no_opts(mock_main_loop):
    # some options are required
    res = invoke_cli_runner(["workspace"], expected_error=True)
    assert "Missing option" in res.output


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_environment(mock_main_loop, mock_get_user_name, temp_project):
    with temporary_directory() as tempdir:
        mock_get_user_name.return_value = "me"

        with patch.object(ProfileConfigProvider, "get_config") as config_mock:
            config_mock.return_value = get_config()
            invoke_cli_runner(["workspace", "-s", tempdir, "-d", "the-repo", "--environment", "default"])

            assert mock_main_loop.call_count == 1
            assert config_mock.call_count == 1
            assert mock_get_user_name.call_count == 1


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_basic_opts(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        mock_get_user_name.return_value = "me"

        invoke_cli_runner(["workspace", "-s", tempdir, "-d", "the-repo"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, WorkspaceClient)
        assert client.base_path == "/Users/me/the-repo"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_unknown_user(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        mock_get_user_name.return_value = None

        res = invoke_cli_runner(["workspace", "-s", tempdir, "-d", "the-repo"], expected_error=True)

        assert mock_main_loop.call_count == 0
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 1

        assert "Destination path can't be automatically determined because the user is" in res.output


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_dry_run(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        invoke_cli_runner(["workspace", "-s", tempdir, "-d", "/Shared/the-repo", "--dry-run"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert not mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, WorkspaceClient)
        assert client.base_path == "/Shared/the-repo"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_polling(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        mock_get_user_name.return_value = "me"

        invoke_cli_runner(["workspace", "-s", tempdir, "-d", "the-repo", "--polling-interval", "2"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["polling_interval_secs"] == 2.0
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, WorkspaceClient)
        assert client.base_path == "/Users/me/the-repo"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_include_dir(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        os.mkdir(os.path.join(tempdir, "foo"))

        invoke_cli_runner(["workspace", "-s", tempdir, "-d", "the-repo", "-u", "me", "-i", "foo"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == ["/foo/"]
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, WorkspaceClient)
        assert client.base_path == "/Users/me/the-repo"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_force_include_dir(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        os.mkdir(os.path.join(tempdir, "foo"))

        invoke_cli_runner(["workspace", "-s", tempdir, "-d", "the-repo", "-u", "me", "-fi", "foo"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == ["/foo/"]
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, WorkspaceClient)
        assert client.base_path == "/Users/me/the-repo"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_include_pattern(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        os.mkdir(os.path.join(tempdir, "foo"))

        invoke_cli_runner(["workspace", "-s", tempdir, "-d", "the-repo", "-u", "me", "-ip", "foo/*.py"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == ["foo/*.py"]
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, WorkspaceClient)
        assert client.base_path == "/Users/me/the-repo"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_force_include_pattern(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        os.mkdir(os.path.join(tempdir, "foo"))

        invoke_cli_runner(["workspace", "-s", tempdir, "-d", "the-repo", "-u", "me", "-fip", "foo/*.py"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == ["foo/*.py"]
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, WorkspaceClient)
        assert client.base_path == "/Users/me/the-repo"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_exclude_dir(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        os.mkdir(os.path.join(tempdir, "foo"))

        invoke_cli_runner(["workspace", "-s", tempdir, "-d", "the-repo", "-u", "me", "-e", "foo"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert sorted(mock_main_loop.call_args[1]["matcher"].ignores) == sorted(DBX_SYNC_DEFAULT_IGNORES + ["/foo/"])
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, WorkspaceClient)
        assert client.base_path == "/Users/me/the-repo"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_exclude_pattern(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        os.mkdir(os.path.join(tempdir, "foo"))

        invoke_cli_runner(["workspace", "-s", tempdir, "-d", "the-repo", "-u", "me", "-ep", "foo/**/*.py"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert sorted(mock_main_loop.call_args[1]["matcher"].ignores) == sorted(
            DBX_SYNC_DEFAULT_IGNORES + ["foo/**/*.py"]
        )
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, WorkspaceClient)
        assert client.base_path == "/Users/me/the-repo"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_include_dir_not_exists(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        # we don't create the "foo" subdir, so it should produce an error

        res = invoke_cli_runner(
            ["workspace", "-s", tempdir, "-d", "the-repo", "-u", "me", "-i", "foo"], expected_error=True
        )

        assert mock_main_loop.call_count == 0
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert "does not exist" in res.output


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_inferred_source(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir, pushd(tempdir):
        os.mkdir(os.path.join(tempdir, ".git"))

        invoke_cli_runner(["workspace", "-d", "the-repo", "-u", "me"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, WorkspaceClient)
        assert client.base_path == "/Users/me/the-repo"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_inferred_source_no_git(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir, pushd(tempdir):
        # source can only be inferred when the cwd contains a .git subdir

        res = invoke_cli_runner(["workspace", "-d", "the-repo", "-u", "me"], expected_error=True)

        assert mock_main_loop.call_count == 0
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert "Must specify source" in res.output


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_allow_delete_unmatched(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        invoke_cli_runner(
            ["workspace", "-s", tempdir, "-d", "the-repo", "-u", "me", "--unmatched-behaviour=allow-delete-unmatched"]
        )

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.ALLOW_DELETE_UNMATCHED

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, WorkspaceClient)
        assert client.base_path == "/Users/me/the-repo"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_workspace_disallow_delete_unmatched(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        invoke_cli_runner(
            [
                "workspace",
                "-s",
                tempdir,
                "-d",
                "the-repo",
                "-u",
                "me",
                "--unmatched-behaviour=disallow-delete-unmatched",
            ]
        )

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]
        assert mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.DISALLOW_DELETE_UNMATCHED

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, WorkspaceClient)
        assert client.base_path == "/Users/me/the-repo"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_dbfs_no_opts(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir, pushd(tempdir):
        # infer source based on cwd having a .git directory
        os.mkdir(os.path.join(tempdir, ".git"))

        mock_get_user_name.return_value = "me"

        # we can run with no options as long as the source and user can be automatically inferred
        invoke_cli_runner(
            [
                "dbfs",
            ]
        )

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, DBFSClient)
        assert client.base_path == f"dbfs:/tmp/users/me/{os.path.basename(tempdir)}"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_dbfs_polling(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir, pushd(tempdir):
        # infer source based on cwd having a .git directory
        os.mkdir(os.path.join(tempdir, ".git"))

        mock_get_user_name.return_value = "me"

        # we can run with no options as long as the source and user can be automatically inferred
        invoke_cli_runner(["dbfs", "--polling-interval", "3"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["polling_interval_secs"] == 3.0
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, DBFSClient)
        assert client.base_path == f"dbfs:/tmp/users/me/{os.path.basename(tempdir)}"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_dbfs_dry_run(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir, pushd(tempdir):
        # infer source based on cwd having a .git directory
        os.mkdir(os.path.join(tempdir, ".git"))

        mock_get_user_name.return_value = "me"

        # we can run with no options as long as the source and user can be automatically inferred
        invoke_cli_runner(["dbfs", "--dry-run"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert not mock_main_loop.call_args[1]["watch"]

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, DBFSClient)
        assert client.base_path == f"dbfs:/tmp/users/me/{os.path.basename(tempdir)}"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_dbfs_source_dest(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        mock_get_user_name.return_value = "me"

        # we can run with no options as long as the source and user can be automatically inferred
        invoke_cli_runner(["dbfs", "-s", tempdir, "-d", "/foo/bar"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, DBFSClient)
        assert client.base_path == f"dbfs:/foo/bar"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_dbfs_specify_user(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir, pushd(tempdir):
        # infer source based on cwd having a .git directory
        os.mkdir(os.path.join(tempdir, ".git"))

        # we can run with no options as long as the source and user can be automatically inferred
        invoke_cli_runner(["dbfs", "-u", "someone"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == []
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert mock_main_loop.call_args[1]["matcher"].ignores == DBX_SYNC_DEFAULT_IGNORES
        assert mock_main_loop.call_args[1]["watch"]

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, DBFSClient)
        assert client.base_path == f"dbfs:/tmp/users/someone/{os.path.basename(tempdir)}"


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_dbfs_unknown_user(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        mock_get_user_name.return_value = None

        # we can run with no options as long as the source and user can be automatically inferred
        res = invoke_cli_runner(["dbfs"], expected_error=True)

        assert mock_main_loop.call_count == 0
        assert mock_get_config.call_count == 1

        assert "Destination path can't be automatically determined because the user is" in res.output


@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_dbfs_no_root(mock_main_loop, mock_get_user_name, mock_get_config):
    with temporary_directory() as tempdir:
        mock_get_user_name.return_value = "me"

        # we can run with no options as long as the source and user can be automatically inferred
        res = invoke_cli_runner(["dbfs", "-d", "/"], expected_error=True)

        assert mock_main_loop.call_count == 0
        assert mock_get_config.call_count == 1

        assert "Destination cannot be the root path" in res.output


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_use_gitignore(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True

        os.mkdir(os.path.join(tempdir, "foo"))

        # .gitignore will be used by default for ignore patterns
        with open(os.path.join(tempdir, ".gitignore"), "w") as gif:
            gif.write("/bar\n")
            gif.write("/baz\n")

        invoke_cli_runner(["repo", "-s", tempdir, "-d", "the-repo", "-u", "me", "-i", "foo"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == ["/foo/"]
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert sorted(mock_main_loop.call_args[1]["matcher"].ignores) == sorted(
            DBX_SYNC_DEFAULT_IGNORES + ["/bar", "/baz"]
        )
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.sync.repo_exists")
@patch("dbx.commands.sync.sync.get_user_name")
@patch("dbx.commands.sync.sync.main_loop")
def test_repo_no_use_gitignore(mock_main_loop, mock_get_user_name, mock_repo_exists, mock_get_config):
    with temporary_directory() as tempdir:
        mock_repo_exists.return_value = True

        os.mkdir(os.path.join(tempdir, "foo"))

        # .gitignore will be used by default for ignore patterns
        with open(os.path.join(tempdir, ".gitignore"), "w") as gif:
            gif.write("/bar\n")
            gif.write("/baz\n")

        invoke_cli_runner(["repo", "-s", tempdir, "-d", "the-repo", "-u", "me", "-i", "foo", "--no-use-gitignore"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_get_user_name.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["matcher"]
        assert mock_main_loop.call_args[1]["matcher"].includes == ["/foo/"]
        assert mock_main_loop.call_args[1]["matcher"].force_includes == []
        assert sorted(mock_main_loop.call_args[1]["matcher"].ignores) == sorted(DBX_SYNC_DEFAULT_IGNORES)
        assert mock_main_loop.call_args[1]["watch"]
        assert (
            mock_main_loop.call_args[1]["delete_unmatched_option"] == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
        )

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


def test_repo_exists():
    client = AsyncMock()
    asyncio.run(repo_exists(client))
    assert client.exists.call_count == 1
