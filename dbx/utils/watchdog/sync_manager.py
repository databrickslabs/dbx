import asyncio
import getpass
import logging
import os
import pathlib
from typing import Optional

import paramiko
import watchdog.events
import watchdog.events as we
from watchdog.observers import Observer
from typing import List, Union
from dbx.utils.common import get_ssh_client
from dbx.utils.watchdog.tunnel_manager import TunnelManager


class SyncHandler(watchdog.events.PatternMatchingEventHandler):
    def __init__(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        super(SyncHandler, self).__init__(ignore_patterns=[
            ".git/*", ".dbx/*", "build/*", "dist/*", "*.egg-info/*", "*__pycache__*"
        ])

        self._queue = queue
        self._loop = loop

    def on_any_event(self, event: we.FileSystemEvent):
        self._loop.call_soon_threadsafe(self._queue.put_nowait, event)


class SyncManager:
    EventList = List[Union[we.FileSystemEvent, we.FileSystemMovedEvent]]

    def __init__(self, tunnel_manager: TunnelManager):
        self._tunnel_manager = tunnel_manager
        self._status = "initializing"
        self._remote_project_path = pathlib.Path(
            f"/databricks/driver/{getpass.getuser()}/{pathlib.Path('').absolute().name}"
        )
        self._observer = Observer()
        self._loop = asyncio.get_event_loop()
        self._queue = asyncio.Queue(loop=self._loop)
        self._handler = SyncHandler(self._queue, self._loop)
        self._observer.schedule(self._handler, path=".", recursive=True)
        self._ssh_client: Optional[paramiko.SSHClient] = None
        self._sftp_client: Optional[paramiko.SFTPClient] = None

    @property
    def status(self):
        return self._status

    @staticmethod
    def _safe_callback(callback, *args):
        try:
            callback(*args)
        except Exception as e:
            logging.error("Exception while executing callback")
            logging.error(e)

    @staticmethod
    def _filter_events_list(all_evs: EventList, filter_type: str) -> EventList:
        dir_evs = [e for e in all_evs if e.event_type == filter_type and e.is_directory]
        if not dir_evs:
            return all_evs
        else:
            resulting_events = []
            for dir_ev in dir_evs:
                for e in all_evs:
                    logging.info(f"Checking event: {e}")
                    if e.event_type == filter_type and not e.is_directory:
                        # skip file-level deletions, because we're going to delete the whole directory
                        if not pathlib.Path(e.src_path).parent == pathlib.Path(dir_ev.src_path):
                            resulting_events.append(e)
                        else:
                            logging.info(
                                f"Skipping file {e.src_path}, because operation will be performed on dir level")
                    else:
                        resulting_events.append(e)
                    logging.info(f"Resulting events state: {resulting_events}")
            return resulting_events

    def _deduplicate_events(self, events: EventList):
        if len([e for e in events if
                e.is_directory and e.event_type in (we.EVENT_TYPE_MOVED, we.EVENT_TYPE_DELETED)]) == 0:
            logging.info("No problematic events provided")
            return events
        else:
            deletions = self._filter_events_list(events, we.EVENT_TYPE_DELETED)
            moves = self._filter_events_list(deletions, we.EVENT_TYPE_MOVED)
            return moves

    def _process_events_batch(self, batch_events: EventList):
        # unfortunately watchdog application makes processing of changes very hard
        # for every change in generates a lot of duplicated and misleading events
        # To avoid this behaviour, we don't use DirModified events at all
        logging.info(f"Initial event list: {batch_events}")
        cleaned_events = [e for e in batch_events if not isinstance(e, we.DirModifiedEvent)]
        deduplicated_events = self._deduplicate_events(cleaned_events)
        logging.info(f"Deduplicated list: {deduplicated_events}")
        for event in deduplicated_events:
            # waiting for pattern matching from https://www.python.org/dev/peps/pep-0622
            parent_path = pathlib.Path(event.src_path).parent

            if parent_path != pathlib.Path("."):
                try:
                    prep_cmd = f"cd {self._remote_project_path}; mkdir -p {parent_path}"
                    logging.info(f"Prep command: {prep_cmd}")
                    self._ssh_client.exec_command(prep_cmd)
                except Exception as e:
                    logging.error("Exception while preparing parent directory")
                    logging.error(e)

            if event.event_type == we.EVENT_TYPE_CREATED and event.is_directory:
                self._safe_callback(self._sftp_client.mkdir, event.src_path)
            elif event.event_type == we.EVENT_TYPE_CREATED and not event.is_directory:
                self._safe_callback(self._sftp_client.put, event.src_path, event.src_path)
            elif event.event_type == we.EVENT_TYPE_DELETED and event.is_directory:
                prep_cmd = f"cd {self._remote_project_path}; rm -rf {event.src_path}"
                self._safe_callback(self._ssh_client.exec_command, prep_cmd)
            elif event.event_type == we.EVENT_TYPE_DELETED and not event.is_directory:
                self._safe_callback(self._sftp_client.remove, event.src_path)
            elif event.event_type == we.EVENT_TYPE_MOVED:
                self._safe_callback(self._sftp_client.rename, event.src_path, event.dest_path)
            elif event.event_type == we.EVENT_TYPE_MODIFIED and not event.is_directory:
                self._safe_callback(self._sftp_client.put, event.src_path, event.src_path)

        # if isinstance(event, we.FileCreatedEvent):
        #
        #     logging.info(prep_cmd)
        # self._ssh_client.exec_command(prep_cmd)
        # self._sftp_client.put(event.src_path, self._relate_to_remote(event.src_path))

    def _relate_to_remote(self, path: str) -> str:
        return str(pathlib.Path(path).relative_to(self._remote_project_path))

    async def sync_routine(self):
        while True:
            if self._tunnel_manager.status.startswith("running"):
                if self._status in ("initializing", "failed", "waiting for tunnel"):
                    self._status = "Preparing SSH & SFTP clients"
                    self._ssh_client = get_ssh_client(self._tunnel_manager.get_tunnel_info())
                    self._sftp_client: paramiko.SFTPClient = self._ssh_client.open_sftp()
                    self._ssh_client.exec_command(f"rm -rf {self._remote_project_path}")
                    self._ssh_client.exec_command(f"mkdir -p {self._remote_project_path}")
                    self._sftp_client.chdir(str(self._remote_project_path))
                    self._status = "Performing initial bootstrap"
                    if self._observer.is_alive():
                        self._observer.stop()
                    self._bootstrap_sync()
                    self._observer.start()
                    await asyncio.sleep(2)
                else:
                    self._status = "running"
                    logging.info("Pulling new batch of fs events")
                    try:
                        events = [await self._queue.get() for _ in range(self._queue.qsize())]
                        self._process_events_batch(events)
                    except Exception as e:
                        logging.error(f"Error during file sync: {e}")
                        self._status = "failed"
                    await asyncio.sleep(1)
            else:
                self._status = "waiting for tunnel"
                await asyncio.sleep(2)

    def _bootstrap_sync(self):
        logging.info("Walking through files in cwd")
        for root, dirs, files in os.walk(os.getcwd()):
            for dir_name in dirs:
                localized_path = str(pathlib.Path(root, dir_name).relative_to(os.getcwd()))
                event = we.DirCreatedEvent(src_path=localized_path)
                self._handler.dispatch(event)
            for file in files:
                localized_path = str(pathlib.Path(root, file).relative_to(os.getcwd()))
                event = we.FileCreatedEvent(src_path=localized_path)
                self._handler.dispatch(event)
        logging.info("Starting observer process")
        logging.info("Observer is launched")
