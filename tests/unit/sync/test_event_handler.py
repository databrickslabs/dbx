import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import List
from unittest.mock import patch, PropertyMock

from watchdog.events import DirCreatedEvent, FileCreatedEvent, FileDeletedEvent, FileModifiedEvent, FileMovedEvent

from dbx.sync.event_handler import CollectingEventHandler, file_watcher
from dbx.sync.path_matcher import PathMatcher

from .utils import temporary_directory


@contextmanager
def temp_event_handler(*, ignores: List[str] = None, includes: List[str] = None, polling_interval_secs: float = None):
    with temporary_directory() as tempdir:
        matcher = PathMatcher(tempdir, includes=includes, ignores=ignores)
        with file_watcher(
            source=tempdir, matcher=matcher, polling_interval_secs=polling_interval_secs
        ) as event_handler:
            yield (event_handler, Path(tempdir))


def get_events(event_handler: CollectingEventHandler, expected: int, *, timeout_seconds: int = 5):
    start_time = time.monotonic()
    all_events = []
    while time.monotonic() < start_time + 5:
        events = event_handler.get_events()
        if events:
            all_events.extend(events)
            if len(all_events) >= expected:
                return all_events
        time.sleep(0.05)
    assert False, "Failed to collect the expected number of events"


def test_event_handler_create_file():
    """
    Tests file_watcher can detect file creation.
    """
    with temp_event_handler(includes=["foo"]) as (event_handler, tempdir):
        (tempdir / "foo").touch()
        events = get_events(event_handler, expected=1)
        assert len(events) == 1
        assert isinstance(events[0], FileCreatedEvent)
        assert events[0].src_path == os.path.join(tempdir, "foo")


def test_event_handler_create_file_polling():
    """
    Tests file_watcher can detect file creation with polling.
    """
    with temp_event_handler(includes=["foo"], polling_interval_secs=0.5) as (event_handler, tempdir):
        (tempdir / "foo").touch()
        events = get_events(event_handler, expected=1)
        assert len(events) == 1
        assert isinstance(events[0], FileCreatedEvent)
        assert events[0].src_path == os.path.join(tempdir, "foo")


@patch("dbx.sync.event_handler.Observer")
def test_event_handler_create_file_polling_fabllack(observer_mock):
    """
    Tests file_watcher can detect file creation when having to fall back to polling.
    """
    cls = observer_mock.return_value
    cls.start.side_effect = OSError("sorry no inotify")

    with temp_event_handler(includes=["foo"]) as (event_handler, tempdir):
        (tempdir / "foo").touch()
        events = get_events(event_handler, expected=1)
        assert len(events) == 1
        assert isinstance(events[0], FileCreatedEvent)
        assert events[0].src_path == os.path.join(tempdir, "foo")


def test_event_handler_create_file_ignored():
    """
    Tests file_watcher can ignore file creation events we want to ignore.
    """
    with temp_event_handler(includes=["foo"]) as (event_handler, tempdir):
        (tempdir / "bar").touch()
        (tempdir / "foo").touch()
        events = get_events(event_handler, expected=1)
        assert len(events) == 1
        assert isinstance(events[0], FileCreatedEvent)
        assert events[0].src_path == os.path.join(tempdir, "foo")


def test_event_handler_create_dir():
    """
    Tests file_watcher can detect directory creation.
    """
    with temp_event_handler(includes=["foo"]) as (event_handler, tempdir):
        (tempdir / "foo").mkdir()
        events = get_events(event_handler, expected=1)
        assert len(events) == 1
        assert isinstance(events[0], DirCreatedEvent)
        assert events[0].src_path == os.path.join(tempdir, "foo")


def test_event_handler_create_dir_ignored():
    """
    Tests file_watcher can ignore directory creation events we want to ignore.
    """
    with temp_event_handler(includes=["foo"]) as (event_handler, tempdir):
        (tempdir / "bar").mkdir()
        (tempdir / "foo").mkdir()
        events = get_events(event_handler, expected=1)
        assert len(events) == 1
        assert isinstance(events[0], DirCreatedEvent)
        assert events[0].src_path == os.path.join(tempdir, "foo")


def test_event_handler_delete_file():
    """
    Tests file_watcher can detect file deletion.
    """
    with temp_event_handler(includes=["foo"]) as (event_handler, tempdir):
        (tempdir / "foo").touch()
        os.remove(tempdir / "foo")
        events = get_events(event_handler, expected=2)
        assert len(events) == 2
        assert isinstance(events[0], FileCreatedEvent)
        assert events[0].src_path == os.path.join(tempdir, "foo")
        assert isinstance(events[1], FileDeletedEvent)
        assert events[1].src_path == os.path.join(tempdir, "foo")


def test_event_handler_delete_file_ignored():
    """
    Tests file_watcher can ignore file deletion events we want to ignore.
    """
    with temp_event_handler(includes=["foo"]) as (event_handler, tempdir):
        (tempdir / "bar").touch()
        os.remove(tempdir / "bar")
        (tempdir / "foo").touch()
        events = get_events(event_handler, expected=1)
        assert len(events) == 1
        assert isinstance(events[0], FileCreatedEvent)
        assert events[0].src_path == os.path.join(tempdir, "foo")


def test_event_handler_modify_file():
    """
    Tests file_watcher can detect file modification.
    """
    with temp_event_handler(includes=["foo"]) as (event_handler, tempdir):
        (tempdir / "foo").touch()
        (tempdir / "foo").touch()
        events = get_events(event_handler, expected=2)
        assert len(events) == 2
        assert isinstance(events[0], FileCreatedEvent)
        assert events[0].src_path == os.path.join(tempdir, "foo")
        assert isinstance(events[1], FileModifiedEvent)
        assert events[1].src_path == os.path.join(tempdir, "foo")


def test_event_handler_modify_file_ignored():
    """
    Tests file_watcher can ignore file modification events we want to ignore.
    """
    with temp_event_handler(includes=["foo"]) as (event_handler, tempdir):
        (tempdir / "bar").touch()
        (tempdir / "bar").touch()
        (tempdir / "foo").touch()
        (tempdir / "foo").touch()
        events = get_events(event_handler, expected=2)
        assert len(events) == 2
        assert isinstance(events[0], FileCreatedEvent)
        assert events[0].src_path == os.path.join(tempdir, "foo")
        assert isinstance(events[1], FileModifiedEvent)
        assert events[1].src_path == os.path.join(tempdir, "foo")


def test_event_handler_move_file():
    """
    Tests file_watcher can detect file moves.
    """
    with temp_event_handler(includes=["foo*"]) as (event_handler, tempdir):
        (tempdir / "foo").touch()
        os.rename(tempdir / "foo", tempdir / "foo2")
        events = get_events(event_handler, expected=2)
        assert len(events) == 2
        assert isinstance(events[0], FileCreatedEvent)
        assert events[0].src_path == os.path.join(tempdir, "foo")
        assert isinstance(events[1], FileMovedEvent)
        assert events[1].src_path == os.path.join(tempdir, "foo")


def test_event_handler_move_file_ignored():
    """
    Tests file_watcher can ignore file move events we want to ignore.
    """
    with temp_event_handler(includes=["foo*"]) as (event_handler, tempdir):
        (tempdir / "bar").touch()
        os.rename(tempdir / "bar", tempdir / "bar2")
        (tempdir / "foo").touch()
        events = get_events(event_handler, expected=1)
        assert len(events) == 1
        assert isinstance(events[0], FileCreatedEvent)
        assert events[0].src_path == os.path.join(tempdir, "foo")


def test_keyboard_interrupt():
    """
    Tests that the keyboard interrupt will cause the file watcher to stop without throwing an error.
    """
    with temporary_directory() as tempdir:
        matcher = PathMatcher(tempdir, includes=None, ignores=None)
        with file_watcher(source=tempdir, matcher=matcher):
            # this should be captured by the file_watcher
            raise KeyboardInterrupt()
