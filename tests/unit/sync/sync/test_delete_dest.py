import asyncio
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock

from dbx.commands.sync import create_path_matcher
from dbx.sync import RemoteSyncer


def test_delete_dest():
    """
    Tests that RemoteSyncer can delete destination directories specified in the includes list.
    """

    client = AsyncMock()
    client.name = "test"
    client.base_path = "/test"
    includes = ["foo"]
    excludes = None
    with TemporaryDirectory() as source, TemporaryDirectory() as state_dir:
        matcher = create_path_matcher(source=source, includes=includes, excludes=excludes)
        syncer = RemoteSyncer(
            client=client,
            source=source,
            dry_run=False,
            includes=includes,
            excludes=excludes,
            delete_dest=False,
            full_sync=False,
            state_dir=state_dir,
            matcher=matcher,
        )

        # create a dir and file to sync
        (Path(source) / "foo").mkdir()
        (Path(source) / "foo" / "bar").touch()

        parent = AsyncMock()
        parent.attach_mock(client, "client")

        # sync the file and dir
        assert asyncio.run(syncer.incremental_copy()) == 2
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1

        assert parent.mock_calls[0][0] == "client.mkdirs"
        assert parent.mock_calls[0][1] == ("foo",)
        assert parent.mock_calls[1][0] == "client.put"
        assert parent.mock_calls[1][1] == ("foo/bar", os.path.join(source, "foo", "bar"))

        # syncing again should result in no additional operations
        assert asyncio.run(syncer.incremental_copy()) == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 1
        assert client.put.call_count == 1

        # new syncer, but this time delete dest dirs
        syncer = RemoteSyncer(
            client=client,
            source=source,
            dry_run=False,
            includes=includes,
            excludes=excludes,
            delete_dest=True,
            full_sync=False,
            state_dir=state_dir,
            matcher=matcher,
        )

        parent = AsyncMock()
        parent.attach_mock(client, "client")

        # sync will delete the foo dir first before recreating it and putting the file
        assert asyncio.run(syncer.incremental_copy()) == 2
        assert client.delete.call_count == 1  # deletes the whole foo directory
        assert client.mkdirs.call_count == 2
        assert client.put.call_count == 2
        assert parent.mock_calls[0][0] == "client.delete"
        assert parent.mock_calls[0][1] == ("foo",)
        assert parent.mock_calls[1][0] == "client.mkdirs"
        assert parent.mock_calls[1][1] == ("foo",)
        assert parent.mock_calls[2][0] == "client.put"
        assert parent.mock_calls[2][1] == ("foo/bar", os.path.join(source, "foo", "bar"))
