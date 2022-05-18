import asyncio
import os
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from dbx.commands.sync import create_path_matcher
from dbx.sync import RemoteSyncer, DeleteUnmatchedOption

from tests.unit.sync.utils import temporary_directory


@pytest.mark.parametrize("confirm_delete,use_option", [(True, False), (False, False), (True, True), (False, True)])
@patch("dbx.sync.click")
def test_unmatched_delete_confirm_yes(mock_click, confirm_delete, use_option):
    """
    Tests that RemoteSyncer can properly handle unmatched deletes.

    Unmatched deletes occur when the filter options (e.g. includes) change in such a way that files/directories
    that were synced in a previous run are no longer matched.  Because the sync state keeps track of what has
    been synced, this would result in the files being deleted in the destination.  This may or may not be what the
    user wants to happen, so we give them the option to decide what to do.

    If they don't specify an option, then we use click.confirm to ask whether they want to delete or not.
    The alternative is to provide the option on the command line, which becomes the delete_unmatched_option param.
    """
    client = AsyncMock()
    client.name = "test"
    client.base_path = "/test"
    with temporary_directory() as source, temporary_directory() as state_dir:

        def create_syncer(*, includes=None, excludes=None):
            matcher = create_path_matcher(source=source, includes=includes, excludes=excludes)
            syncer_opts = dict(
                client=client,
                source=source,
                dry_run=False,
                includes=includes,
                excludes=excludes,
                full_sync=False,
                state_dir=state_dir,
                matcher=matcher,
            )

            if use_option:
                delete_unmatched_option = (
                    DeleteUnmatchedOption.ALLOW_DELETE_UNMATCHED
                    if confirm_delete
                    else DeleteUnmatchedOption.DISALLOW_DELETE_UNMATCHED
                )
                syncer_opts["delete_unmatched_option"] = delete_unmatched_option

            return RemoteSyncer(**syncer_opts)

        syncer = create_syncer(includes=["foo"])

        # initially no files
        op_count = syncer.incremental_copy()
        assert op_count == 0
        assert client.delete.call_count == 0
        assert client.mkdirs.call_count == 0
        assert client.put.call_count == 0

        # create two files, but only one will sync
        (Path(source) / "foo").touch()
        (Path(source) / "bar").touch()

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

        # recreate syncer with different includes, which would result in foo being deleted
        syncer = create_syncer(includes=["bar"])

        # confirm that we want to delete the files
        mock_click.confirm.return_value = confirm_delete

        if confirm_delete:
            # syncing again should result in a delete and put
            assert syncer.incremental_copy() == 2
            assert client.delete.call_count == 1
            assert client.mkdirs.call_count == 0
            assert client.put.call_count == 2
        else:
            # syncing again should result in only a put, as we ignore the previous file
            assert syncer.incremental_copy() == 1
            assert client.delete.call_count == 0
            assert client.mkdirs.call_count == 0
            assert client.put.call_count == 2

        mock_click.confirm.call_count == (1 if not use_option else 0)
