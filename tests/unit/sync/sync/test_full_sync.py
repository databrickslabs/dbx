import asyncio
import os
from pathlib import Path
from unittest.mock import AsyncMock

from dbx.commands.sync import create_path_matcher
from dbx.sync import RemoteSyncer

from tests.unit.sync.utils import temporary_directory


def test_initial_full_sync():
    """
    Tests that RemoteSyncer can perform a full sync under the scenario where it is the first time
    encountering the files.
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
            full_sync=True,
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


def test_full_sync_after_initial_sync():
    """
    Tests that RemoteSyncer can perform a full sync after the files have previously been synced.
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

        # creating a new syncer should result in no more ops because the state dir is used
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

        # sync again.  no more ops.
        assert syncer.incremental_copy() == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1

        # but, if we set full sync, then the initial sync will repeat the operations again
        syncer = RemoteSyncer(
            client=client,
            source=source,
            dry_run=False,
            includes=None,
            excludes=None,
            full_sync=True,
            state_dir=state_dir,
            matcher=matcher,
        )

        parent = AsyncMock()
        parent.attach_mock(client, "client")

        # sync again.  same operations repeated again.
        assert syncer.incremental_copy() == 2
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 2
        assert client.put.call_count == 2
        assert parent.mock_calls[0][0] == "client.mkdirs"
        assert parent.mock_calls[0][1] == ("foo",)
        assert parent.mock_calls[1][0] == "client.put"
        assert parent.mock_calls[1][1] == ("foo/bar", os.path.join(source, "foo", "bar"))


def test_dry_run_full_sync_after_initial_sync():
    """
    Tests that RemoteSyncer can perform a dry-run of a full sync after an initial sync.
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

        # run full sync in dry run mode.  no operations should happen.
        syncer = RemoteSyncer(
            client=client,
            source=source,
            dry_run=True,
            includes=None,
            excludes=None,
            full_sync=True,
            state_dir=state_dir,
            matcher=matcher,
        )

        parent = AsyncMock()
        parent.attach_mock(client, "client")

        # sync again.  no additional operations should be made.
        assert syncer.incremental_copy() == 2
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1
