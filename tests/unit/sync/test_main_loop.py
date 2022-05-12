import tempfile
from unittest.mock import MagicMock, patch

from dbx.commands.sync import main_loop

from .utils import temporary_directory


@patch("dbx.commands.sync.RemoteSyncer")
def test_main_loop_no_watch(remote_syncer_class_mock):
    with temporary_directory() as source:
        client = MagicMock()
        remote_sync_mock = MagicMock()
        remote_syncer_class_mock.return_value = remote_sync_mock
        remote_sync_mock.incremental_copy.return_value = 0
        main_loop(
            source=source,
            client=client,
            full_sync=False,
            dry_run=False,
            includes=None,
            excludes=None,
            watch=False,
        )

        assert remote_sync_mock.incremental_copy.call_count == 1

        assert remote_syncer_class_mock.call_args[1]["source"] == source
        assert remote_syncer_class_mock.call_args[1]["client"] == client
        assert not remote_syncer_class_mock.call_args[1]["full_sync"]
        assert not remote_syncer_class_mock.call_args[1]["dry_run"]
        assert not remote_syncer_class_mock.call_args[1]["includes"]
        assert not remote_syncer_class_mock.call_args[1]["excludes"]
        assert remote_syncer_class_mock.call_args[1]["matcher"] is not None


@patch("dbx.commands.sync.RemoteSyncer")
def test_main_loop_dry_run(remote_syncer_class_mock):
    with temporary_directory() as source:
        client = MagicMock()
        remote_sync_mock = MagicMock()
        remote_syncer_class_mock.return_value = remote_sync_mock
        remote_sync_mock.incremental_copy.return_value = 5
        main_loop(
            source=source,
            client=client,
            full_sync=False,
            dry_run=True,
            includes=None,
            excludes=None,
            watch=False,
        )

        assert remote_sync_mock.incremental_copy.call_count == 1

        assert remote_syncer_class_mock.call_args[1]["source"] == source
        assert remote_syncer_class_mock.call_args[1]["client"] == client
        assert not remote_syncer_class_mock.call_args[1]["full_sync"]
        assert remote_syncer_class_mock.call_args[1]["dry_run"]
        assert not remote_syncer_class_mock.call_args[1]["includes"]
        assert not remote_syncer_class_mock.call_args[1]["excludes"]
        assert remote_syncer_class_mock.call_args[1]["matcher"] is not None


@patch("dbx.commands.sync.file_watcher")
@patch("dbx.commands.sync.RemoteSyncer")
def test_main_loop_watch(remote_syncer_class_mock, mock_file_watcher):
    with temporary_directory() as source:
        client = MagicMock()
        remote_sync_mock = MagicMock()
        remote_syncer_class_mock.return_value = remote_sync_mock

        # incremental_copy should be called four times, with the last time breaking out of the loop.
        remote_sync_mock.incremental_copy.side_effect = [1, 1, 1, -1]

        mock_file_watch_result = MagicMock()
        mock_event_handler = MagicMock()
        mock_file_watch_result.__enter__ = MagicMock(return_value=mock_event_handler)
        mock_file_watch_result.__exit__ = MagicMock(return_value=False)
        mock_event_handler.get_events.return_value = ["event1", "event2", "event3"]

        mock_file_watcher.return_value = mock_file_watch_result

        main_loop(
            source=source,
            client=client,
            full_sync=False,
            dry_run=False,
            includes=None,
            excludes=None,
            watch=True,
        )

        assert remote_sync_mock.incremental_copy.call_count == 4
        assert mock_event_handler.get_events.call_count == 3

        assert remote_syncer_class_mock.call_args[1]["source"] == source
        assert remote_syncer_class_mock.call_args[1]["client"] == client
        assert not remote_syncer_class_mock.call_args[1]["full_sync"]
        assert not remote_syncer_class_mock.call_args[1]["dry_run"]
        assert not remote_syncer_class_mock.call_args[1]["includes"]
        assert not remote_syncer_class_mock.call_args[1]["excludes"]
        assert remote_syncer_class_mock.call_args[1]["matcher"] is not None


@patch("dbx.commands.sync.file_watcher")
@patch("dbx.commands.sync.RemoteSyncer")
def test_main_loop_watch_no_events(remote_syncer_class_mock, mock_file_watcher):
    """
    Test running the main loop with one of the get_events calls returning no events, resulting in a short sleep.
    """

    with temporary_directory() as source:
        client = MagicMock()
        remote_sync_mock = MagicMock()
        remote_syncer_class_mock.return_value = remote_sync_mock

        # incremental_copy should be called three times, with the last time breaking out of the loop.
        remote_sync_mock.incremental_copy.side_effect = [1, 1, -1]

        mock_file_watch_result = MagicMock()
        mock_event_handler = MagicMock()
        mock_file_watch_result.__enter__ = MagicMock(return_value=mock_event_handler)
        mock_file_watch_result.__exit__ = MagicMock(return_value=False)
        mock_event_handler.get_events.side_effect = [
            # no events the first time, so incremental_copy isn't called
            [],
            # events for the second call, so incremental copy is called
            ["event1"],
            # events for the third call, so incremental copy is called
            ["event2", "event3", "event4"],
        ]

        mock_file_watcher.return_value = mock_file_watch_result

        main_loop(
            source=source,
            client=client,
            full_sync=False,
            dry_run=False,
            includes=None,
            excludes=None,
            watch=True,
            sleep_interval=0.05,
        )

        assert remote_sync_mock.incremental_copy.call_count == 3
        assert mock_event_handler.get_events.call_count == 3

        assert remote_syncer_class_mock.call_args[1]["source"] == source
        assert remote_syncer_class_mock.call_args[1]["client"] == client
        assert not remote_syncer_class_mock.call_args[1]["full_sync"]
        assert not remote_syncer_class_mock.call_args[1]["dry_run"]
        assert not remote_syncer_class_mock.call_args[1]["includes"]
        assert not remote_syncer_class_mock.call_args[1]["excludes"]
        assert remote_syncer_class_mock.call_args[1]["matcher"] is not None
