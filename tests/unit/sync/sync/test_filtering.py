import asyncio
import logging
import os
from pathlib import Path
from unittest.mock import AsyncMock

from dbx.commands.sync import create_path_matcher
from dbx.sync import RemoteSyncer

from tests.unit.sync.utils import temporary_directory

logger = logging.getLogger(__name__)


def test_include():
    """
    Tests that includes can be used to limit which directories are synced over.
    """

    client = AsyncMock()
    client.name = "test"
    client.base_path = "/test"
    with temporary_directory() as source, temporary_directory() as state_dir:
        includes = ["foo"]
        excludes = None
        matcher = create_path_matcher(source=source, includes=includes, excludes=excludes)
        syncer = RemoteSyncer(
            client=client,
            source=source,
            dry_run=False,
            includes=includes,
            excludes=excludes,
            full_sync=False,
            state_dir=state_dir,
            matcher=matcher,
        )

        # create a dir and file to sync
        (Path(source) / "foo").mkdir()
        (Path(source) / "foo" / "bar").touch()

        # and some dirs and files that should be ignored
        (Path(source) / "baz").touch()
        (Path(source) / "bar").mkdir()
        (Path(source) / "bar" / "baz").touch()

        parent = AsyncMock()
        parent.attach_mock(client, "client")

        # sync the file and dir
        assert syncer.incremental_copy() == 2
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1
        assert parent.mock_calls[0][0] == "client.mkdirs"
        assert parent.mock_calls[0][1] == ("foo",)
        assert parent.mock_calls[1][0] == "client.put"
        assert parent.mock_calls[1][1] == ("foo/bar", os.path.join(source, "foo", "bar"))

        # syncing again should result in no additional operations
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1


def test_default_ignore_git():
    """
    Tests that a .git directory is ignored by default.
    """

    client = AsyncMock()
    client.name = "test"
    client.base_path = "/test"
    with temporary_directory() as source, temporary_directory() as state_dir:
        includes = None
        excludes = None
        matcher = create_path_matcher(source=source, includes=includes, excludes=excludes)
        syncer = RemoteSyncer(
            client=client,
            source=source,
            dry_run=False,
            includes=includes,
            excludes=excludes,
            full_sync=False,
            state_dir=state_dir,
            matcher=matcher,
        )

        # create a dir and file to sync
        (Path(source) / "foo").mkdir()
        (Path(source) / "foo" / "bar").touch()

        # and some dirs and files that should be ignored
        (Path(source) / ".git").mkdir()
        (Path(source) / ".git" / "foo").touch()

        parent = AsyncMock()
        parent.attach_mock(client, "client")

        # sync the file and dir
        assert syncer.incremental_copy() == 2
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1
        assert parent.mock_calls[0][0] == "client.mkdirs"
        assert parent.mock_calls[0][1] == ("foo",)
        assert parent.mock_calls[1][0] == "client.put"
        assert parent.mock_calls[1][1] == ("foo/bar", os.path.join(source, "foo", "bar"))

        # syncing again should result in no additional operations
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1


def test_exclude():
    """
    Tests that excludes can be used to prevent certain directories from being synced over.
    """

    client = AsyncMock()
    client.name = "test"
    client.base_path = "/test"
    with temporary_directory() as source, temporary_directory() as state_dir:
        includes = None
        excludes = ["baz"]
        matcher = create_path_matcher(source=source, includes=includes, excludes=excludes)
        syncer = RemoteSyncer(
            client=client,
            source=source,
            dry_run=False,
            includes=includes,
            excludes=excludes,
            full_sync=False,
            state_dir=state_dir,
            matcher=matcher,
        )

        # create a dir and file to sync
        (Path(source) / "foo").mkdir()
        (Path(source) / "foo" / "bar").touch()

        # and some dirs and files that should be ignored
        (Path(source) / "baz").mkdir()
        (Path(source) / "baz" / "bar").touch()

        parent = AsyncMock()
        parent.attach_mock(client, "client")

        # sync the file and dir
        assert syncer.incremental_copy() == 2
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1
        assert parent.mock_calls[0][0] == "client.mkdirs"
        assert parent.mock_calls[0][1] == ("foo",)
        assert parent.mock_calls[1][0] == "client.put"
        assert parent.mock_calls[1][1] == ("foo/bar", os.path.join(source, "foo", "bar"))

        # syncing again should result in no additional operations
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1


def test_include_deeply_nested():
    """
    Tests that includes can be used to limit which directories are synced over and that it works
    with deeply nested files.
    """

    client = AsyncMock()
    client.name = "test"
    client.base_path = "/test"
    with temporary_directory() as source, temporary_directory() as state_dir:
        includes = ["foo/bar/baz"]
        excludes = None
        matcher = create_path_matcher(source=source, includes=includes, excludes=excludes)
        syncer = RemoteSyncer(
            client=client,
            source=source,
            dry_run=False,
            includes=includes,
            excludes=excludes,
            full_sync=False,
            state_dir=state_dir,
            matcher=matcher,
        )

        # create a dir and file to sync
        (Path(source) / "foo").mkdir()
        (Path(source) / "foo" / "bar").mkdir()
        (Path(source) / "foo" / "bar" / "baz").mkdir()
        (Path(source) / "foo" / "bar" / "baz" / "bop").touch()

        # and some dirs and files that should be ignored
        (Path(source) / "baz").touch()
        (Path(source) / "bar").mkdir()
        (Path(source) / "bar" / "baz").touch()

        parent = AsyncMock()
        parent.attach_mock(client, "client")

        # sync the file and dir
        assert syncer.incremental_copy() == 4
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 3
        assert client.put.call_count == 1
        assert parent.mock_calls[0][0] == "client.mkdirs"
        assert parent.mock_calls[0][1] == ("foo",)
        assert parent.mock_calls[1][0] == "client.mkdirs"
        assert parent.mock_calls[1][1] == ("foo/bar",)
        assert parent.mock_calls[2][0] == "client.mkdirs"
        assert parent.mock_calls[2][1] == ("foo/bar/baz",)
        assert parent.mock_calls[3][0] == "client.put"
        assert parent.mock_calls[3][1] == ("foo/bar/baz/bop", os.path.join(source, "foo", "bar", "baz", "bop"))

        # syncing again should result in no additional operations
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 3
        assert client.put.call_count == 1
