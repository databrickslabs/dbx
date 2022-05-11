import os
from tempfile import TemporaryDirectory
from unittest.mock import patch, call, MagicMock

from dbx.commands.sync import check, dbfs, repo
from dbx.sync.clients import DBFSClient, ReposClient

from tests.unit.utils import invoke_cli_runner

from .utils import temporary_directory, pushd


@patch("dbx.commands.sync.get_databricks_config")
def test_check(mock_get_config):
    invoke_cli_runner(check, [])
    assert mock_get_config.call_count == 1
    assert mock_get_config.call_args == call(None)


@patch("dbx.commands.sync.get_databricks_config")
def test_check_with_profile(mock_get_config):
    invoke_cli_runner(check, ["--profile", "foo"])
    assert mock_get_config.call_count == 1
    assert mock_get_config.call_args == call("foo")


@patch("dbx.commands.sync.get_databricks_config")
@patch("dbx.commands.sync.main_loop")
def test_repo_no_opts(mock_get_config, mock_main_loop):
    # some options are required
    res = invoke_cli_runner(repo, [], expected_error=True)
    assert "Missing option" in res.output


@patch("dbx.commands.sync.main_loop")
@patch("dbx.commands.sync.get_databricks_config")
def test_repo_basic_opts(mock_get_config, mock_main_loop):
    with temporary_directory() as tempdir:
        config = MagicMock()
        mock_get_config.return_value = config
        invoke_cli_runner(repo, ["-s", tempdir, "-d", "the-repo", "-u", "me"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["includes"] == []
        assert mock_main_loop.call_args[1]["excludes"] == []
        assert mock_main_loop.call_args[1]["watch"]

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.main_loop")
@patch("dbx.commands.sync.get_databricks_config")
def test_repo_dry_run(mock_get_config, mock_main_loop):
    with temporary_directory() as tempdir:
        config = MagicMock()
        mock_get_config.return_value = config
        invoke_cli_runner(repo, ["-s", tempdir, "-d", "the-repo", "-u", "me", "--dry-run"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["includes"] == []
        assert mock_main_loop.call_args[1]["excludes"] == []
        assert not mock_main_loop.call_args[1]["watch"]

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.main_loop")
@patch("dbx.commands.sync.get_databricks_config")
def test_repo_include_dir(mock_get_config, mock_main_loop):
    with temporary_directory() as tempdir:

        os.mkdir(os.path.join(tempdir, "foo"))

        config = MagicMock()
        mock_get_config.return_value = config
        invoke_cli_runner(repo, ["-s", tempdir, "-d", "the-repo", "-u", "me", "-i", "foo"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["includes"] == ["/foo/"]
        assert mock_main_loop.call_args[1]["excludes"] == []
        assert mock_main_loop.call_args[1]["watch"]

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.main_loop")
@patch("dbx.commands.sync.get_databricks_config")
def test_repo_exclude_dir(mock_get_config, mock_main_loop):
    with temporary_directory() as tempdir:

        os.mkdir(os.path.join(tempdir, "foo"))

        config = MagicMock()
        mock_get_config.return_value = config
        invoke_cli_runner(repo, ["-s", tempdir, "-d", "the-repo", "-u", "me", "-e", "foo"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["includes"] == []
        assert mock_main_loop.call_args[1]["excludes"] == ["/foo/"]
        assert mock_main_loop.call_args[1]["watch"]

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.main_loop")
@patch("dbx.commands.sync.get_databricks_config")
def test_repo_include_dir_not_exists(mock_get_config, mock_main_loop):
    with temporary_directory() as tempdir:

        # we don't create the "foo" subdir, so it should produce an error

        config = MagicMock()
        mock_get_config.return_value = config
        res = invoke_cli_runner(repo, ["-s", tempdir, "-d", "the-repo", "-u", "me", "-i", "foo"], expected_error=True)

        assert mock_main_loop.call_count == 0
        assert mock_get_config.call_count == 1

        assert "does not exist" in res.output


@patch("dbx.commands.sync.main_loop")
@patch("dbx.commands.sync.get_databricks_config")
def test_repo_inferred_source(mock_get_config, mock_main_loop):
    with temporary_directory() as tempdir, pushd(tempdir):
        os.mkdir(os.path.join(tempdir, ".git"))

        config = MagicMock()
        mock_get_config.return_value = config
        invoke_cli_runner(repo, ["-d", "the-repo", "-u", "me"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["includes"] == []
        assert mock_main_loop.call_args[1]["excludes"] == []
        assert mock_main_loop.call_args[1]["watch"]

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, ReposClient)
        assert client.base_path == "/Repos/me/the-repo"


@patch("dbx.commands.sync.main_loop")
@patch("dbx.commands.sync.get_databricks_config")
def test_repo_inferred_source_no_git(mock_get_config, mock_main_loop):
    with temporary_directory() as tempdir, pushd(tempdir):

        # source can only be inferred when the cwd contains a .git subdir

        config = MagicMock()
        mock_get_config.return_value = config
        res = invoke_cli_runner(repo, ["-d", "the-repo", "-u", "me"], expected_error=True)

        assert mock_main_loop.call_count == 0
        assert mock_get_config.call_count == 1

        assert "Must specify source" in res.output


@patch("dbx.commands.sync.getuser")
@patch("dbx.commands.sync.main_loop")
@patch("dbx.commands.sync.get_databricks_config")
def test_dbfs_no_opts(mock_get_config, mock_main_loop, mock_getuser):
    with temporary_directory() as tempdir, pushd(tempdir):

        # infer source based on cwd having a .git directory
        os.mkdir(os.path.join(tempdir, ".git"))

        config = MagicMock()
        mock_get_config.return_value = config
        mock_getuser.return_value = "me"

        # we can run with no options as long as the source and user can be automatically inferred
        invoke_cli_runner(dbfs, [])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["includes"] == []
        assert mock_main_loop.call_args[1]["excludes"] == []
        assert mock_main_loop.call_args[1]["watch"]

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, DBFSClient)
        assert client.base_path == f"dbfs:/tmp/users/me/{os.path.basename(tempdir)}"


@patch("dbx.commands.sync.getuser")
@patch("dbx.commands.sync.main_loop")
@patch("dbx.commands.sync.get_databricks_config")
def test_dbfs_dry_run(mock_get_config, mock_main_loop, mock_getuser):
    with temporary_directory() as tempdir, pushd(tempdir):

        # infer source based on cwd having a .git directory
        os.mkdir(os.path.join(tempdir, ".git"))

        config = MagicMock()
        mock_get_config.return_value = config
        mock_getuser.return_value = "me"

        # we can run with no options as long as the source and user can be automatically inferred
        invoke_cli_runner(dbfs, ["--dry-run"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["includes"] == []
        assert mock_main_loop.call_args[1]["excludes"] == []
        assert not mock_main_loop.call_args[1]["watch"]

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, DBFSClient)
        assert client.base_path == f"dbfs:/tmp/users/me/{os.path.basename(tempdir)}"


@patch("dbx.commands.sync.getuser")
@patch("dbx.commands.sync.main_loop")
@patch("dbx.commands.sync.get_databricks_config")
def test_dbfs_source_dest(mock_get_config, mock_main_loop, mock_getuser):
    with temporary_directory() as tempdir:

        config = MagicMock()
        mock_get_config.return_value = config
        mock_getuser.return_value = "me"

        # we can run with no options as long as the source and user can be automatically inferred
        invoke_cli_runner(dbfs, ["-s", tempdir, "-d", "/foo/bar"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["includes"] == []
        assert mock_main_loop.call_args[1]["excludes"] == []
        assert mock_main_loop.call_args[1]["watch"]

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, DBFSClient)
        assert client.base_path == f"dbfs:/foo/bar"


@patch("dbx.commands.sync.getuser")
@patch("dbx.commands.sync.main_loop")
@patch("dbx.commands.sync.get_databricks_config")
def test_dbfs_specify_user(mock_get_config, mock_main_loop, mock_getuser):
    with temporary_directory() as tempdir, pushd(tempdir):

        # infer source based on cwd having a .git directory
        os.mkdir(os.path.join(tempdir, ".git"))

        config = MagicMock()
        mock_get_config.return_value = config

        # we can run with no options as long as the source and user can be automatically inferred
        invoke_cli_runner(dbfs, ["-u", "someone"])

        assert mock_main_loop.call_count == 1
        assert mock_get_config.call_count == 1
        assert mock_getuser.call_count == 0

        assert mock_main_loop.call_args[1]["source"] == tempdir
        assert not mock_main_loop.call_args[1]["full_sync"]
        assert not mock_main_loop.call_args[1]["dry_run"]
        assert mock_main_loop.call_args[1]["includes"] == []
        assert mock_main_loop.call_args[1]["excludes"] == []
        assert mock_main_loop.call_args[1]["watch"]

        client = mock_main_loop.call_args[1]["client"]

        assert isinstance(client, DBFSClient)
        assert client.base_path == f"dbfs:/tmp/users/someone/{os.path.basename(tempdir)}"


@patch("dbx.commands.sync.getuser")
@patch("dbx.commands.sync.main_loop")
@patch("dbx.commands.sync.get_databricks_config")
def test_dbfs_unknown_user(mock_get_config, mock_main_loop, mock_getuser):
    with temporary_directory() as tempdir:

        config = MagicMock()
        mock_get_config.return_value = config
        mock_getuser.return_value = None

        # we can run with no options as long as the source and user can be automatically inferred
        res = invoke_cli_runner(dbfs, [], expected_error=True)

        assert mock_main_loop.call_count == 0
        assert mock_get_config.call_count == 1

        assert "Destination path can't be automatically determined because the user is not known" in res.output


@patch("dbx.commands.sync.getuser")
@patch("dbx.commands.sync.main_loop")
@patch("dbx.commands.sync.get_databricks_config")
def test_dbfs_no_root(mock_get_config, mock_main_loop, mock_getuser):
    with temporary_directory() as tempdir:

        config = MagicMock()
        mock_get_config.return_value = config
        mock_getuser.return_value = "me"

        # we can run with no options as long as the source and user can be automatically inferred
        res = invoke_cli_runner(dbfs, ["-d", "/"], expected_error=True)

        assert mock_main_loop.call_count == 0
        assert mock_get_config.call_count == 1

        assert "Destination cannot be the root path" in res.output
