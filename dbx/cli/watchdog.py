from watchdog.observers import Observer
import watchdog.events
import watchdog.observers
from watchdog.events import (
    FileSystemEvent,
    FileCreatedEvent, FileModifiedEvent, FileMovedEvent, FileDeletedEvent,
    DirDeletedEvent, DirMovedEvent
)
import time
from dbx.cli.utils import TunnelInfo, dbx_echo, get_ssh_client
import pathlib
import os
import paramiko
import getpass
from typing import Union
import logging
from tqdm import tqdm

logging.basicConfig()


class RsyncHandler(watchdog.events.FileSystemEventHandler):

    def _put_file(self, event):
        if isinstance(event, (FileCreatedEvent, FileModifiedEvent)):
            local_path = pathlib.Path(event.src_path)
            remote_path = self._remote_project_path.joinpath(event.src_path)
            self._ssh_client.exec_command("mkdir -p %s" % remote_path.parent)
            self._sftp_client.put(str(local_path.absolute()), str(remote_path))

    def __init__(self,
                 ssh_client: paramiko.client.SSHClient,
                 sftp_client: paramiko.SFTPClient,
                 remote_project_path: pathlib.Path
                 ):
        super(RsyncHandler, self).__init__()
        self._ssh_client = ssh_client
        self._sftp_client = sftp_client
        self._remote_project_path = remote_project_path

    def on_created(self, event: FileSystemEvent):
        if isinstance(event, FileModifiedEvent):
            dbx_echo("Created: %s" % event.src_path)
            self._put_file(event)

    def on_modified(self, event: FileSystemEvent):
        if isinstance(event, FileModifiedEvent):
            dbx_echo("Modified: %s" % event.src_path)
            self._put_file(event)

    def on_moved(self, event: Union[DirMovedEvent, FileMovedEvent]):
        dbx_echo(f"Moving {event.src_path} to {event.dest_path}")
        remote_src_path = self._remote_project_path.joinpath(event.src_path)
        remote_dest_path = self._remote_project_path.joinpath(event.dest_path)
        self._ssh_client.exec_command(f"mv -R {remote_src_path} {remote_dest_path}")

    def on_deleted(self, event: Union[DirDeletedEvent, FileDeletedEvent]):
        remote_src_path = self._remote_project_path.joinpath(event.src_path)
        if isinstance(event, DirDeletedEvent):
            dbx_echo("Deleting directory: %s" % event.src_path)
            self._sftp_client.rmdir(str(remote_src_path))
        else:
            dbx_echo("Deleting file: %s" % event.src_path)
            self._sftp_client.remove(str(remote_src_path))


class Rsync:

    def _initial_sync(self):
        dbx_echo("Performing initial synchronization into remote directory: %s" % self._remote_project_path)
        current_dir = pathlib.Path('.').absolute()

        # collecting nested file list and flattening it
        all_files = sum([[(head_path, f) for f in files] for head_path, _, files in os.walk(current_dir)], [])

        for head_path, f in tqdm(all_files, "Initial sync process: "):
            local_path = pathlib.Path(head_path, f)
            rel_path = local_path.relative_to(current_dir)
            remote_path = self._remote_project_path.joinpath(rel_path)
            self._ssh_client.exec_command("mkdir -p %s" % remote_path.parent)
            self._sftp_client.put(str(local_path.absolute()), str(remote_path))
        dbx_echo("Initial synchronization finished")

    @staticmethod
    def _get_remote_project_path():
        return pathlib.Path(f"/databricks/driver/{getpass.getuser()}/{pathlib.Path('.').absolute().name}")

    def _prepare_remote(self):
        self._ssh_client.exec_command(f"rm -rf {self._remote_project_path}")
        self._ssh_client.exec_command(f"mkdir -p {self._remote_project_path}")

    def __init__(self, tunnel_info: TunnelInfo):
        self._ssh_client = get_ssh_client(tunnel_info)
        self._sftp_client: paramiko.SFTPClient = self._ssh_client.open_sftp()
        self._remote_project_path = self._get_remote_project_path()
        self._prepare_remote()
        self._initial_sync()
        self.observer = watchdog.observers.Observer()
        handler = RsyncHandler(self._ssh_client, self._sftp_client, self._remote_project_path)
        self.observer.schedule(handler, path=".", recursive=True)
        dbx_echo("Starting directory synchronization via tunnel, all file changes will be saved to cluster")
        self.observer.start()

    def launch(self):
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()
