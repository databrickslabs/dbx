import os
import threading
from contextlib import contextmanager
from functools import partial
from typing import List

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserverVFS

from dbx.utils import dbx_echo

from .path_matcher import PathMatcher, filtered_listdir


class CollectingEventHandler(FileSystemEventHandler):
    """A watchdog event handler that collects all the events captured so they can be retrieved in batches."""

    def __init__(self, *, matcher: PathMatcher = None):
        super().__init__()

        self.lock = threading.RLock()
        self.events = []
        self.matcher = matcher

    def _should_ignore(self, event: FileSystemEvent) -> bool:
        return not self.matcher.match(event.src_path, is_directory=event.is_directory) if self.matcher else False

    def get_events(self) -> List[FileSystemEvent]:
        """Gets the events collected since the last time this method was called.

        Returns:
            List[FileSystemEvent]: events collected since the last call
        """
        with self.lock:
            result = self.events
            self.events = []
            return result

    def on_moved(self, event: FileSystemEvent) -> None:
        with self.lock:
            super().on_moved(event)
            if not self._should_ignore(event):
                self.events.append(event)

    def on_created(self, event: FileSystemEvent) -> None:
        with self.lock:
            super().on_created(event)
            if not self._should_ignore(event):
                self.events.append(event)

    def on_deleted(self, event: FileSystemEvent) -> None:
        with self.lock:
            super().on_deleted(event)
            if not self._should_ignore(event):
                self.events.append(event)

    def on_modified(self, event: FileSystemEvent) -> None:
        with self.lock:
            super().on_modified(event)
            if not self._should_ignore(event):
                self.events.append(event)


@contextmanager
def file_watcher(*, source: str, matcher: PathMatcher, polling_interval_secs: float = None):
    """Watches a source directory for changes to files, filtered by the given path matcher.

    This yields an event handler that can be used to retrieve file events within the context of this
    context manager.

    For example,

        event_handler.get_events()

    will return all the file events that occurred since the last time it was called.

    Args:
        source (str): source directory to watch for changes
        matcher (PathMatcher): used to identify which files to pay attention to and which to ignore
        polling_interval_secs (float): enabling polling for file system changes instead of using file system events
                                       by setting the interval in seconds between polling the file system.

    Yields:
        CollectingEventHandler: the event handler which collects together all the file events
    """

    event_handler = CollectingEventHandler(matcher=matcher)

    observer = None

    if not polling_interval_secs:
        try:
            observer = Observer()
            observer.schedule(event_handler, source, recursive=True)
            observer.start()
        except OSError:
            dbx_echo("Failed to start file watcher.  Falling back to polling-based observer.")
            observer = None

    if not observer:
        if not polling_interval_secs:
            polling_interval_secs = 1.0
        dbx_echo(f"Starting file system polling with {polling_interval_secs} second polling interval")
        observer = PollingObserverVFS(
            listdir=partial(filtered_listdir, matcher), stat=os.stat, polling_interval=polling_interval_secs
        )
        observer.schedule(event_handler, source, recursive=True)
        observer.start()

    try:
        yield event_handler
    except KeyboardInterrupt:
        pass
    observer.stop()
    observer.join()
