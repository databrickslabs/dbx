import asyncio
import os
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from dbx.commands.sync import create_path_matcher
from dbx.sync import RemoteSyncer, get_relative_path

from tests.unit.sync.utils import temporary_directory


def test_empty_dir():
    """
    Tests that RemoteSyncer works with an empty directory.
    """
    client = MagicMock()
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
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 0

        # stil no files
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 0


def test_state_dir_creation():
    """
    Tests that RemoteSyncer creates the state directory.
    """
    client = MagicMock()
    client.name = "test"
    client.base_path = "/test"
    with temporary_directory() as source, temporary_directory() as state_dir:
        matcher = create_path_matcher(source=source, includes=None, excludes=None)

        # define a new state dir, but don't create it
        state_dir = Path(state_dir) / "state" / "really" / "nested"

        RemoteSyncer(
            client=client,
            source=source,
            dry_run=False,
            includes=None,
            excludes=None,
            full_sync=False,
            state_dir=state_dir,
            matcher=matcher,
        )

        assert os.path.exists(state_dir)


def test_single_file_put_and_delete():
    """
    Tests that RemoteSyncer can sync a file after it is created and then delete it after it is deleted.
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

        # create a file to sync
        (Path(source) / "foo").touch()

        # sync the file
        assert syncer.incremental_copy() == 1
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 1

        # should put the file remotely
        assert client.put.call_args_list[0][0] == ("foo", os.path.join(source, "foo"))

        # syncing again should result in no additional operations
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 1

        # remove the file locally, which should cause it to be removed remotely
        os.remove(Path(source) / "foo")

        assert syncer.incremental_copy() == 1
        assert client.delete.call_count == 1
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 1

        # should delete the remote file
        assert client.delete.call_args_list[0][0] == ("foo",)

        # syncing again should result in no additional operations
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 1
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 1


def test_put_dir_and_file_and_delete():
    """
    Tests that RemoteSyncer can sync a directory after it is created and then delete it after it is deleted.
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
        (Path(source) / "foo" / "bar").touch()

        parent = AsyncMock()
        parent.attach_mock(client, "client")

        # directory and file should be created in the proper order
        assert syncer.incremental_copy() == 2
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1
        assert parent.mock_calls[0][0] == "client.mkdirs"
        assert parent.mock_calls[0][1] == ("foo",)
        assert parent.mock_calls[1][0] == "client.put"
        assert parent.mock_calls[1][1] == ("foo/bar", os.path.join(source, "foo", "bar"))

        # sync again.  no more ops.
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1

        # deleting the parent directory should result in the dir and file deleted by just the directory
        # delete call.
        parent = AsyncMock()
        parent.attach_mock(client, "client")
        shutil.rmtree(Path(source) / "foo")
        assert syncer.incremental_copy() == 1
        assert client.delete.call_count == 1
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1
        assert parent.mock_calls[0][0] == "client.delete"
        assert parent.mock_calls[0][1] == ("foo",)

        # sync again.  no more ops.
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 1
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1


def test_single_file_put_twice():
    """
    Tests that RemoteSyncer can sync a file after it is created and then put it again after another update.
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

        # create a file to sync
        (Path(source) / "foo").touch()

        # sync the file
        assert syncer.incremental_copy() == 1
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 1

        # should put the file remotely
        assert client.put.call_args_list[0][0] == ("foo", os.path.join(source, "foo"))

        # syncing again should result in no additional operations
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 1

        # modify the file
        with open(Path(source) / "foo", "w") as f:
            f.write("blah")

        assert syncer.incremental_copy() == 1
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 2


def test_get_relative_path():
    assert get_relative_path("/foo/bar", "/foo/bar/baz") == "baz"
    assert get_relative_path("/foo/bar/", "/foo/bar/baz") == "baz"
    assert get_relative_path("/foo/bar/", "/foo/bar/baz/") == "baz"

    assert get_relative_path("/foo/bar", "/foo/bar/baz/bop") == "baz/bop"

    with pytest.raises(ValueError):
        assert get_relative_path("/foo/bar", "/foo/bar")
    with pytest.raises(ValueError):
        assert get_relative_path("/foo/bar", "/foo/bar/")
    with pytest.raises(ValueError):
        assert get_relative_path("/foo/bar/baz", "/foo/bar")


def test_corrupt_state():
    """
    Tests that RemoteSyncer can sync a file after it is created and then delete it after it is deleted.
    """
    client = AsyncMock()
    client.name = "test"
    client.base_path = "/test"
    with temporary_directory() as source, temporary_directory() as state_dir:
        matcher = create_path_matcher(source=source, includes=None, excludes=None)

        def _create_syncer():
            return RemoteSyncer(
                client=client,
                source=source,
                dry_run=False,
                includes=None,
                excludes=None,
                full_sync=False,
                state_dir=state_dir,
                matcher=matcher,
            )

        syncer = _create_syncer()

        # initially no files
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 0

        # create a file to sync
        (Path(source) / "foo").touch()

        # sync the file
        assert syncer.incremental_copy() == 1
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 1

        # sync again, no ops because already synced
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 1

        # new syncer will restore state
        syncer = _create_syncer()

        # sync again, no ops because already synced
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 1

        # corrupt the pickled state
        with open(syncer.snapshot_path, "wb") as f:
            f.write(b"asdf")

        # new syncer will restore state but fail, resulting in empty state
        syncer = _create_syncer()

        # sync again, put happens again due to empty state
        assert syncer.incremental_copy() == 1
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 2
