import asyncio
import hashlib
import os
import pickle
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Union

import aiohttp
import click
from watchdog.utils.dirsnapshot import DirectorySnapshot, DirectorySnapshotDiff, EmptyDirectorySnapshot

from dbx.utils import dbx_echo

from .clients import BaseClient
from .constants import DBX_SYNC_DIR
from .path_matcher import PathMatcher
from .snapshot import compute_snapshot_diff


def is_dir_ancestor(possible_ancestor: str, path: str) -> str:
    return possible_ancestor == os.path.commonpath([possible_ancestor, path])


def get_relative_path(ancestor: str, path: str):
    ancestor = ancestor.rstrip("/")
    path = path.rstrip("/")
    if not is_dir_ancestor(ancestor, path):
        raise ValueError(f"{ancestor} is not an ancestor of {path}")
    return path[len(ancestor) + 1:]


def with_depth(d: str) -> int:
    """Compute the depth of a directory and prepend it as a tuple.
    This is used to sort directories by depth.

    Args:
        d (str): directory

    Returns:
        int: depth of directory
    """
    return (len(d.split("/")), d)


def get_snapshot_name(client: BaseClient) -> str:
    """Gets the path that will be used to save sync snapshots for the given client.
    Each client writes to a particular destination type, like repos or dbfs, and uses
    a particular base path.  These together determine the name of the state file.

    Args:
        client (BaseClient): client used for syncing

    Returns:
        str: name of the state file
    """
    dir_name = client.base_path.split("/")[-1]
    base_path_hash = hashlib.md5(client.base_path.encode("utf-8")).hexdigest()
    return f"{dir_name}-{client.name}-{base_path_hash}"


class RemoteSyncer:
    def __init__(
        self,
        *,
        client: BaseClient,
        source: str,
        dry_run: bool,
        matcher: PathMatcher,
        includes: List[str],
        excludes: List[str],
        delete_dest: bool,
        full_sync: bool = False,
        max_parallel: int = 4,
        state_dir: Union[Path, str] = DBX_SYNC_DIR,
    ):
        # State directory should be relative to the directory we're syncing from.  Usually this will
        # be the same directory the tool is run from, but you can specify a different source directory.
        state_dir = Path(source) / Path(state_dir)

        self.includes = includes
        self.excludes = excludes
        self.client = client
        self.source = source
        self.state_dir = state_dir
        self.delete_dest = delete_dest
        self.full_sync = full_sync
        self.dry_run = dry_run
        self.max_parallel = max_parallel
        self.is_first_sync = True
        self.tempdir = TemporaryDirectory().name  # noqa
        self.matcher = matcher
        self.snapshot_path = os.path.join(state_dir, get_snapshot_name(client))
        self.last_snapshot = None

        if not os.path.exists(state_dir):
            os.makedirs(state_dir)

        if self.dry_run:
            dbx_echo("Performing a dry run")

        if self.delete_dest:
            self.full_sync = True
            dbx_echo("Performing a full sync due to deleting destination directories")
        elif self.full_sync:
            dbx_echo("Performing a full sync")

    async def _delete_dest_directories(self):
        # Delete the destination directories in case they already exist.  We only delete those
        # directories that are included in the sync.
        # TODO This should techincally delete the base destination directory when there are no includes specified.
        if not self.includes:
            raise click.BadArgumentUsage("Cannot delete destination directories if no includes specified")
        dbx_echo("Deleting destination included directories")
        connector = aiohttp.TCPConnector()
        async with aiohttp.ClientSession(connector=connector) as session:
            for include in self.includes:
                await self.client.delete(include, session=session, recursive=True)

    async def _apply_dirs_created(self, diff: DirectorySnapshotDiff, session: aiohttp.ClientSession) -> None:
        op_count = 0

        # Sort dirs by depth so we can create directories in parallel as much as possible.
        dirs_created = [with_depth(d) for d in diff.dirs_created]
        dirs_created = sorted(dirs_created, key=lambda p: p[0])

        curr_depth = None
        tasks = []
        for depth, path in dirs_created:
            op_count += 1
            if not self.dry_run:

                if curr_depth is None:
                    curr_depth = depth

                # When we're about to create a directory at the next depth, wait for any existing
                # directory creations to complete.  We want to make sure parents are created before children.
                if depth > curr_depth and tasks:
                    await asyncio.gather(*tasks)
                    curr_depth = depth
                    tasks = []

                tasks.append(self.client.mkdirs(get_relative_path(self.source, path), session=session))

            else:
                dbx_echo(f"(noop) Dir created: {path}")
        if tasks:
            await asyncio.gather(*tasks)
        return op_count

    async def _apply_dirs_deleted(
        self, diff: DirectorySnapshotDiff, session: aiohttp.ClientSession, deleted_dirs: List[str]
    ) -> None:
        op_count = 0

        # Sort dirs by depth so we can delete the higher level directories first.  Because deletes are
        # recursive, this will probably save us some work by eliminating deeper directories that
        # need to be deleted.
        dirs_deleted = [with_depth(d) for d in diff.dirs_deleted]
        dirs_deleted = sorted(dirs_deleted, key=lambda p: p[0])

        for _, path in sorted(dirs_deleted):
            # If an ancestor has been deleted, then we don't need to delete because it would have already
            # been deleted.
            if not any(is_dir_ancestor(deleted_path, path) for deleted_path in deleted_dirs):
                op_count += 1
                if not self.dry_run:
                    await self.client.delete(get_relative_path(self.source, path), session=session, recursive=True)
                    deleted_dirs.append(path)
                else:
                    dbx_echo(f"(noop) Dir deleted: {path}")
        return op_count

    async def _apply_files_created(self, diff: DirectorySnapshotDiff, session: aiohttp.ClientSession) -> None:
        tasks = []
        op_count = 0
        for path in sorted(diff.files_created):
            op_count += 1
            if not self.dry_run:
                # Files can be created in parallel.
                tasks.append(self.client.put(get_relative_path(self.source, path), path, session=session))
            else:
                dbx_echo(f"(noop) File created: {path}")
        if tasks:
            await asyncio.gather(*tasks)
        return op_count

    async def _apply_files_deleted(
        self, diff: DirectorySnapshotDiff, session: aiohttp.ClientSession, deleted_dirs: List[str]
    ) -> None:
        tasks = []
        op_count = 0
        for path in sorted(diff.files_deleted):
            # No need to delete a file if an ancestor directory has already been deleted, as deletes
            # are recursive.
            if not any(is_dir_ancestor(deleted_path, path) for deleted_path in deleted_dirs):
                op_count += 1
                if not self.dry_run:
                    # Files can be deleted in parallel.
                    tasks.append(self.client.delete(get_relative_path(self.source, path), session=session))
                else:
                    dbx_echo(f"(noop) File deleted: {path}")
        if tasks:
            await asyncio.gather(*tasks)
        return op_count

    async def _apply_files_modified(self, diff: DirectorySnapshotDiff, session: aiohttp.ClientSession) -> None:
        tasks = []
        op_count = 0
        for path in sorted(diff.files_modified):
            op_count += 1
            if not self.dry_run:
                # Files can be created in parallel.
                tasks.append(self.client.put(get_relative_path(self.source, path), path, session=session))
            else:
                dbx_echo(f"(noop) File modified: {path}")
        if tasks:
            await asyncio.gather(*tasks)
        return op_count

    async def _apply_snapshot_diff(self, diff: DirectorySnapshotDiff, session: aiohttp.ClientSession) -> int:
        op_count = 0

        # Collects directories deleted while applying this diff.  Since we perform recursive deletes, we can
        # avoid making some delete operations if an ancestor has already been deleted.
        deleted_dirs = []

        op_count += await self._apply_dirs_deleted(diff, session, deleted_dirs)
        op_count += await self._apply_dirs_created(diff, session)
        op_count += await self._apply_files_created(diff, session)
        op_count += await self._apply_files_deleted(diff, session, deleted_dirs)
        op_count += await self._apply_files_modified(diff, session)

        return op_count

    async def incremental_copy(self) -> int:
        """
        Performs an incremental copy from source using the client.

        Returns the number of operations performed or would have been performed (if dry run).
        """

        if self.is_first_sync:
            self.is_first_sync = False

            if self.delete_dest and not self.dry_run:
                await self._delete_dest_directories()

            if self.full_sync:
                if not self.dry_run and os.path.exists(self.snapshot_path):
                    os.remove(self.snapshot_path)
                self.last_snapshot = EmptyDirectorySnapshot()
            else:
                if os.path.exists(self.snapshot_path):
                    dbx_echo(f"Restoring sync snapshot from {self.snapshot_path}")
                    with open(self.snapshot_path, "rb") as f:
                        try:
                            self.last_snapshot = pickle.load(f)
                        except pickle.UnpicklingError:
                            dbx_echo("Failed to restore sync state.  Starting from clean state.")
                            self.last_snapshot = EmptyDirectorySnapshot()
                else:
                    self.last_snapshot = EmptyDirectorySnapshot()

        # When walking the directory tree, we use the ignore spec to do an initial first pass at excluding
        # paths that should be definitely ignored.  Good examples of this are the .git folder, if it exists.
        # This makes the directory walk more efficient.

        def _filtered_listdir(root):
            for entry in os.scandir(root):
                entry_name = os.path.join(root, entry if isinstance(entry, str) else entry.name)
                # Some paths are definitely ignored due to an ignore spec.  These should not be traversed.
                if not self.matcher.should_ignore(entry_name, is_directory=os.path.isdir(entry_name)):
                    yield entry

        snapshot = DirectorySnapshot(self.source, listdir=_filtered_listdir)

        # Now that we've walked the full tree, apply the path matcher to each path, which applies both
        # the include and ignores rules.
        matched_paths = {}
        unmatched_paths = {}
        for path, st in snapshot._stat_info.items():
            if self.matcher.match(path, is_directory=snapshot.isdir(path)):
                matched_paths[path] = st
            else:
                # Hold on to the unmatched paths, because we may need to include them due to them
                # being ancestor directories of paths that are matched.
                unmatched_paths[path] = st

        # Make sure that all ancestor directories are included as well, even if not explicitly included
        # via a rule.  This ensures that all ancestor directories will be created.
        additional_matched_paths = {}
        for path, st in unmatched_paths.items():
            # We assume the target directory being synced to at this point already exists and doesn't need
            # to be created.
            if path.rstrip("/") == self.source.rstrip("/"):
                continue
            for matched_path in matched_paths:
                if matched_path.startswith(path) and os.path.commonpath([path, matched_path]) == path:
                    additional_matched_paths[path] = st

        # Replace the snapshot's path dictionary with the newly filtered set.
        snapshot._stat_info = {**matched_paths, **additional_matched_paths}

        diff = compute_snapshot_diff(ref=self.last_snapshot, snapshot=snapshot)

        connector = aiohttp.TCPConnector(limit=self.max_parallel)
        async with aiohttp.ClientSession(connector=connector) as session:
            op_count = await self._apply_snapshot_diff(diff, session)

        self.last_snapshot = snapshot

        # These aren't needed anymore because the directory walk has been completed, plus it prevents pickling.
        self.last_snapshot.listdir = None
        self.last_snapshot.stat = None

        if not self.dry_run:
            with open(self.snapshot_path, "wb") as f:
                pickle.dump(self.last_snapshot, f)

        return op_count
