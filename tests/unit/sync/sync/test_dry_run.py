import asyncio
import shutil
from pathlib import Path
from unittest.mock import AsyncMock

from dbx.commands.sync import create_path_matcher
from dbx.sync import RemoteSyncer

from tests.unit.sync.utils import temporary_directory


def test_single_file_put():
    """
    Tests that RemoteSyncer can perform a dry-run when a single file has been created.
    """
    client = AsyncMock()
    client.name = "test"
    client.base_path = "/test"
    with temporary_directory() as source, temporary_directory() as state_dir:
        matcher = create_path_matcher(source=source, includes=None, excludes=None)
        syncer = RemoteSyncer(
            client=client,
            source=source,
            dry_run=True,
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

        # sync the file.  because it's a dry run we don't copy anything.
        assert syncer.incremental_copy() == 1
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 0


def test_mkdir_put():
    """
    Tests that RemoteSyncer can perform a dry-run when a single file and directory have been created.
    """
    client = AsyncMock()
    client.name = "test"
    client.base_path = "/test"
    with temporary_directory() as source, temporary_directory() as state_dir:
        matcher = create_path_matcher(source=source, includes=None, excludes=None)
        syncer = RemoteSyncer(
            client=client,
            source=source,
            dry_run=True,
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
        (Path(source) / "foo").mkdir()
        (Path(source) / "foo" / "bar").touch()

        # sync the file and dir.  because it's a dry run we don't copy anything.
        assert syncer.incremental_copy() == 2
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 0


def test_delete():
    """
    Tests that RemoteSyncer can perform a dry-run when a directory and file are deleted.
    """
    client = AsyncMock()
    client.name = "test"
    client.base_path = "/test"
    with temporary_directory() as source, temporary_directory() as state_dir:
        matcher = create_path_matcher(source=source, includes=None, excludes=None)

        # first syncer does not do dry run
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
        (Path(source) / "foo").mkdir()
        (Path(source) / "foo" / "bar").touch()

        # sync the file.  because it's a dry run we don't copy anything.
        assert syncer.incremental_copy() == 2
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1

        # sync again.  should be nothing else to sync.
        assert syncer.incremental_copy() == 0

        # second syncer does do dry run
        syncer = RemoteSyncer(
            client=client,
            source=source,
            dry_run=True,
            includes=None,
            excludes=None,
            full_sync=False,
            state_dir=state_dir,
            matcher=matcher,
        )

        # delete the directory and its file
        shutil.rmtree(Path(source) / "foo")

        # sync the file.  because it's a dry run we don't delete anything for real.
        assert syncer.incremental_copy() == 2
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1


def test_single_file_put_twice():
    """
    Tests that RemoteSyncer can perform a dry-run when a single file has been created and then modified.
    """
    client = AsyncMock()
    client.name = "test"
    client.base_path = "/test"
    with temporary_directory() as source, temporary_directory() as state_dir:
        matcher = create_path_matcher(source=source, includes=None, excludes=None)
        syncer = RemoteSyncer(
            client=client,
            source=source,
            dry_run=True,
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

        # sync the file.  because it's a dry run we don't copy anything.
        assert syncer.incremental_copy() == 1
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 0

        # modify if
        with open(Path(source) / "foo", "w") as f:
            f.write("blah")

        # sync the file.  because it's a dry run we don't copy anything.
        assert syncer.incremental_copy() == 1
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 0
