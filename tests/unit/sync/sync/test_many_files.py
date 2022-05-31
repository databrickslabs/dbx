import asyncio
import os
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock

from dbx.commands.sync import create_path_matcher
from dbx.sync import RemoteSyncer

from tests.unit.sync.utils import temporary_directory


def test_syncing_many_files():
    """
    Tests that RemoteSyncer can be used to sync many files with deeply nested folders.
    """

    client = AsyncMock()
    client.name = "test"
    client.base_path = "/test"
    with temporary_directory() as source, temporary_directory() as state_dir:
        matcher = create_path_matcher(source=source, includes=None, excludes=None)
        syncer = RemoteSyncer(
            client=client,
            source=source,
            dry_run=False,
            includes=None,
            excludes=None,
            full_sync=False,
            state_dir=state_dir,
            matcher=matcher,
        )

        # initially no files
        op_count = syncer.incremental_copy()
        assert op_count == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 0

        # create a directory and a file in that directory
        (Path(source) / "foo").mkdir()
        (Path(source) / "foo" / "bar").mkdir()
        (Path(source) / "foo" / "bar" / "baz1").touch()
        (Path(source) / "foo" / "bar" / "baz2").touch()
        (Path(source) / "foo" / "bar" / "baz3").touch()
        (Path(source) / "bar").mkdir()
        (Path(source) / "baz").mkdir()
        (Path(source) / "baz" / "foo1").touch()
        (Path(source) / "baz" / "foo2").touch()
        (Path(source) / "baz" / "foo3").touch()

        parent = AsyncMock()
        parent.attach_mock(client, "client")

        # directory and file should be created in the proper order
        assert syncer.incremental_copy() == 10
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 4
        assert client.put.call_count == 6

        mock_calls = iter(parent.mock_calls)
        next_call = next(mock_calls)
        assert next_call[0] == "client.mkdirs"
        assert next_call[1] == ("bar",)
        next_call = next(mock_calls)
        assert next_call[0] == "client.mkdirs"
        assert next_call[1] == ("baz",)
        next_call = next(mock_calls)
        assert next_call[0] == "client.mkdirs"
        assert next_call[1] == ("foo",)
        next_call = next(mock_calls)
        assert next_call[0] == "client.mkdirs"
        assert next_call[1] == ("foo/bar",)
        next_call = next(mock_calls)
        assert next_call[0] == "client.put"
        assert next_call[1] == ("baz/foo1", os.path.join(source, "baz", "foo1"))
        next_call = next(mock_calls)
        assert next_call[0] == "client.put"
        assert next_call[1] == ("baz/foo2", os.path.join(source, "baz", "foo2"))
        next_call = next(mock_calls)
        assert next_call[0] == "client.put"
        assert next_call[1] == ("baz/foo3", os.path.join(source, "baz", "foo3"))
        next_call = next(mock_calls)
        assert next_call[0] == "client.put"
        assert next_call[1] == ("foo/bar/baz1", os.path.join(source, "foo", "bar", "baz1"))
        next_call = next(mock_calls)
        assert next_call[0] == "client.put"
        assert next_call[1] == ("foo/bar/baz2", os.path.join(source, "foo", "bar", "baz2"))
        next_call = next(mock_calls)
        assert next_call[0] == "client.put"
        assert next_call[1] == ("foo/bar/baz3", os.path.join(source, "foo", "bar", "baz3"))

        # sync again.  no more ops.
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 4
        assert client.put.call_count == 6

        # delete a dir and its files, and create new files
        (Path(source) / "bar" / "foo1").touch()
        (Path(source) / "bar" / "foo2").touch()
        (Path(source) / "bar" / "foo3").touch()
        shutil.rmtree(Path(source) / "baz")
        (Path(source) / "bop").touch()

        # deleting the parent directory should result in the dir and file deleted in the proper order
        parent = AsyncMock()
        parent.attach_mock(client, "client")
        assert syncer.incremental_copy() == 5
        # only need to delete the parent dir
        assert client.delete.call_count == 1
        assert client.mkdirs.call_count == 4
        assert client.put.call_count == 10
        mock_calls = iter(parent.mock_calls)
        next_call = next(mock_calls)
        assert next_call[0] == "client.delete"
        assert next_call[1] == ("baz",)
        next_call = next(mock_calls)
        assert next_call[0] == "client.put"
        assert next_call[1] == ("bar/foo1", os.path.join(source, "bar", "foo1"))
        next_call = next(mock_calls)
        assert next_call[0] == "client.put"
        assert next_call[1] == ("bar/foo2", os.path.join(source, "bar", "foo2"))
        next_call = next(mock_calls)
        assert next_call[0] == "client.put"
        assert next_call[1] == ("bar/foo3", os.path.join(source, "bar", "foo3"))
        next_call = next(mock_calls)
        assert next_call[0] == "client.put"
        assert next_call[1] == ("bop", os.path.join(source, "bop"))

        # sync again.  no more ops.
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 1
        assert client.mkdirs.call_count == 4
        assert client.put.call_count == 10


def test_syncing_many_flat_dirs():
    """
    Tests that RemoteSyncer can be used to sync many directories.
    """

    client = AsyncMock()
    client.name = "test"
    client.base_path = "/test"
    with temporary_directory() as source, temporary_directory() as state_dir:
        matcher = create_path_matcher(source=source, includes=None, excludes=None)
        syncer = RemoteSyncer(
            client=client,
            source=source,
            dry_run=False,
            includes=None,
            excludes=None,
            full_sync=False,
            state_dir=state_dir,
            matcher=matcher,
        )

        # initially no files
        op_count = syncer.incremental_copy()
        assert op_count == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 0

        # create a directory and a file in that directory
        (Path(source) / "foo").mkdir()
        (Path(source) / "bar").mkdir()
        (Path(source) / "baz").mkdir()
        (Path(source) / "bop").mkdir()
        (Path(source) / "boo").mkdir()

        parent = AsyncMock()
        parent.attach_mock(client, "client")

        # directories should be created in the proper order
        assert syncer.incremental_copy() == 5
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 5
        assert client.put.call_count == 0

        mock_calls = iter(parent.mock_calls)
        next_call = next(mock_calls)
        assert next_call[0] == "client.mkdirs"
        assert next_call[1] == ("bar",)
        next_call = next(mock_calls)
        assert next_call[0] == "client.mkdirs"
        assert next_call[1] == ("baz",)
        next_call = next(mock_calls)
        assert next_call[0] == "client.mkdirs"
        assert next_call[1] == ("boo",)
        next_call = next(mock_calls)
        assert next_call[0] == "client.mkdirs"
        assert next_call[1] == ("bop",)
        next_call = next(mock_calls)
        assert next_call[0] == "client.mkdirs"
        assert next_call[1] == ("foo",)

        # now remove all the directories
        (Path(source) / "foo").rmdir()
        (Path(source) / "bar").rmdir()
        (Path(source) / "baz").rmdir()
        (Path(source) / "bop").rmdir()
        (Path(source) / "boo").rmdir()

        parent = AsyncMock()
        parent.attach_mock(client, "client")

        # directories should be deleted in the proper order
        assert syncer.incremental_copy() == 5
        assert client.delete.call_count == 5
        assert client.mkdirs.call_count == 5
        assert client.put.call_count == 0

        mock_calls = iter(parent.mock_calls)
        next_call = next(mock_calls)
        assert next_call[0] == "client.delete"
        assert next_call[1] == ("bar",)
        next_call = next(mock_calls)
        assert next_call[0] == "client.delete"
        assert next_call[1] == ("baz",)
        next_call = next(mock_calls)
        assert next_call[0] == "client.delete"
        assert next_call[1] == ("boo",)
        next_call = next(mock_calls)
        assert next_call[0] == "client.delete"
        assert next_call[1] == ("bop",)
        next_call = next(mock_calls)
        assert next_call[0] == "client.delete"
        assert next_call[1] == ("foo",)


def test_syncing_many_nested_dirs():
    """
    Tests that RemoteSyncer can be used to sync many nested directories.
    """

    client = AsyncMock()
    client.name = "test"
    client.base_path = "/test"
    with temporary_directory() as source, temporary_directory() as state_dir:
        matcher = create_path_matcher(source=source, includes=None, excludes=None)
        syncer = RemoteSyncer(
            client=client,
            source=source,
            dry_run=False,
            includes=None,
            excludes=None,
            full_sync=False,
            state_dir=state_dir,
            matcher=matcher,
        )

        # initially no files
        op_count = syncer.incremental_copy()
        assert op_count == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 0

        # create a directory and a file in that directory
        (Path(source) / "foo").mkdir()
        (Path(source) / "foo" / "bar").mkdir()
        (Path(source) / "foo" / "bar" / "baz").mkdir()

        parent = AsyncMock()
        parent.attach_mock(client, "client")

        # directories should be created in the proper order
        assert syncer.incremental_copy() == 3
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 3
        assert client.put.call_count == 0

        mock_calls = iter(parent.mock_calls)
        next_call = next(mock_calls)
        assert next_call[0] == "client.mkdirs"
        assert next_call[1] == ("foo",)
        next_call = next(mock_calls)
        assert next_call[0] == "client.mkdirs"
        assert next_call[1] == ("foo/bar",)
        next_call = next(mock_calls)
        assert next_call[0] == "client.mkdirs"
        assert next_call[1] == ("foo/bar/baz",)

        # now remove all the directories
        shutil.rmtree(Path(source) / "foo")

        parent = AsyncMock()
        parent.attach_mock(client, "client")

        # directories should be deleted in the proper order
        assert syncer.incremental_copy() == 1
        assert client.delete.call_count == 1
        assert client.mkdirs.call_count == 3
        assert client.put.call_count == 0

        mock_calls = iter(parent.mock_calls)
        next_call = next(mock_calls)
        assert next_call[0] == "client.delete"
        assert next_call[1] == ("foo",)
