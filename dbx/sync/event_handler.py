import os
import threading
from contextlib import contextmanager
from typing import List

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserverVFS

from dbx.utils import dbx_echo

from .path_matcher import PathMatcher


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
def file_watcher(*, source: str, matcher: PathMatcher):
    """Watches a source directory for changes to files, filtered by the given path matcher.

    This yields an event handler that can be used to retrieve file events within the context of this
    context manager.

    For example,

        event_handler.get_events()

    will return all the file events that occurred since the last time it was called.

    Args:
        source (str): source directory to watch for changes
        matcher (PathMatcher): used to identify which files to pay attention to and which to ignore

    Yields:
        CollectingEventHandler: the event handler which collects together all the file events
    """

    def _filtered_listdir(root):
        for entry in os.scandir(root):
            entry_name = os.path.join(root, entry if isinstance(entry, str) else entry.name)
            # Some paths are definitely ignored due to an ignore spec.  These should not be traversed.
            if not matcher.should_ignore(entry_name, is_directory=os.path.isdir(entry_name)):
                yield entry

    event_handler = CollectingEventHandler(matcher=matcher)

    try:
        observer = Observer()
        observer.schedule(event_handler, source, recursive=True)
        observer.start()
    except OSError:
        dbx_echo(f"Failed to start {Observer.__name__}.  Falling back to polling-based observer.")
        observer = PollingObserverVFS(listdir=_filtered_listdir, stat=os.stat)
        observer.schedule(event_handler, source, recursive=True)
        observer.start()

    try:
        yield event_handler
    except KeyboardInterrupt:
        pass
    observer.stop()
    observer.join()
