import asyncio
import hashlib
import os
import pickle
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Union

import aiohttp
import click
from watchdog.utils.dirsnapshot import DirectorySnapshot, EmptyDirectorySnapshot

from dbx.utils import dbx_echo

from .clients import BaseClient
from .constants import DBX_SYNC_DIR
from .path_matcher import PathMatcher
from .snapshot import SnapshotDiff, compute_snapshot_diff


class DeleteUnmatchedOption(Enum):
    ALLOW_DELETE_UNMATCHED = 1
    DISALLOW_DELETE_UNMATCHED = 2
    UNSPECIFIED_DELETE_UNMATCHED = 3


def is_dir_ancestor(possible_ancestor: str, path: str) -> str:
    return possible_ancestor == os.path.commonpath([possible_ancestor, path])


def get_relative_path(ancestor: str, path: str):
    ancestor = str(Path(ancestor))
    path = str(Path(path))
    if not is_dir_ancestor(ancestor, path):
        raise ValueError(f"{ancestor} is not an ancestor of {path}")
    return Path(path[len(ancestor) + 1 :]).as_posix()


def with_depth(d: str) -> int:
    """Compute the depth of a directory and prepend it as a tuple.
    This is used to sort directories by depth.

    Args:
        d (str): directory

    Returns:
        int: depth of directory
    """
    return (len(Path(d).as_posix().split("/")), d)


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
        full_sync: bool = False,
        max_parallel: int = 4,
        state_dir: Union[Path, str] = DBX_SYNC_DIR,
        delete_unmatched_option: DeleteUnmatchedOption = DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED,
    ):
        # State directory should be relative to the directory we're syncing from.  Usually this will
        # be the same directory the tool is run from, but you can specify a different source directory.
        state_dir = Path(source) / Path(state_dir)

        self.includes = includes
        self.excludes = excludes
        self.client = client
        self.source = source
        self.state_dir = state_dir
        self.full_sync = full_sync
        self.dry_run = dry_run
        self.max_parallel = max_parallel
        self.is_first_sync = True
        self.tempdir = TemporaryDirectory().name  # noqa
        self.matcher = matcher
        self.snapshot_path = os.path.join(state_dir, get_snapshot_name(client))
        self.last_snapshot = None
        self.delete_unmatched_option = delete_unmatched_option

        if not os.path.exists(state_dir):
            os.makedirs(state_dir)

        if self.dry_run:
            dbx_echo("Performing a dry run")

        if self.full_sync:
            dbx_echo("Performing a full sync")

    async def _apply_dirs_created(self, diff: SnapshotDiff, session: aiohttp.ClientSession) -> None:
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
        self, diff: SnapshotDiff, session: aiohttp.ClientSession, deleted_dirs: List[str]
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

    async def _apply_files_created(self, diff: SnapshotDiff, session: aiohttp.ClientSession) -> None:
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
        self, diff: SnapshotDiff, session: aiohttp.ClientSession, deleted_dirs: List[str]
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

    async def _apply_files_modified(self, diff: SnapshotDiff, session: aiohttp.ClientSession) -> None:
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

    async def _apply_snapshot_diff(self, diff: SnapshotDiff, session: aiohttp.ClientSession) -> int:
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

    def _remove_unmatched_deletes(self, diff: SnapshotDiff) -> SnapshotDiff:
        """Creates a new snapshot diff where file and directory deletes that don't match the current
        filters are removed.  If this diff is applied then these files/directories won't be deleted
        in the remote location.

        Args:
            diff (SnapshotDiff): diff to update

        Returns:
            SnapshotDiff: new diff with unmatched deletes removed
        """

        return SnapshotDiff(
            files_created=diff.files_created,
            files_modified=diff.files_modified,
            dirs_created=diff.dirs_created,
            dirs_deleted=[d for d in diff.dirs_deleted if self.matcher.match(d, is_directory=True)],
            files_deleted=[f for f in diff.files_deleted if self.matcher.match(f, is_directory=False)],
        )

    async def _dryrun_snapshot_diff_unmatched_deletes(self, diff: SnapshotDiff) -> int:
        """Performs a dry run on only the unmatched file and directory deletes.  These are paths that would
        not be matched by the current filters.  This dry run provides useful logging to the user to understand
        what is happening.

        Unmatched deletes happen when the user switches the include/exclude options between different runs.
        This results in files/directories being removed in the remote that don't match the filters, which may
        or may not be what the user wants.

        Args:
            diff (SnapshotDiff): diff to perform dry run with

        Returns:
            int: number of operations that would have been performed on unmatched deletes
        """
        prev_dry_run = self.dry_run
        try:
            self.dry_run = True

            # Modify the diff to only include files and directories being deleted that don't match the current
            # filter.  This implies that these are only being deleted because they're not in the filter.
            # We should confirm with the user that this is what they want to do.

            dirs_deleted = [d for d in diff.dirs_deleted if not self.matcher.match(d, is_directory=True)]
            files_deleted = [f for f in diff.files_deleted if not self.matcher.match(f, is_directory=False)]

            diff = SnapshotDiff(
                files_created=[],
                files_modified=[],
                dirs_created=[],
                dirs_deleted=dirs_deleted,
                files_deleted=files_deleted,
            )

            op_count = 0

            # Dry run, so we don't need a session.
            session = None

            deleted_dirs = []

            op_count += await self._apply_dirs_deleted(diff, session, deleted_dirs)
            op_count += await self._apply_files_deleted(diff, session, deleted_dirs)

            return op_count
        finally:
            self.dry_run = prev_dry_run

    def _prepare_snapshot(self) -> DirectorySnapshot:
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
            if Path(path) == Path(self.source):
                continue
            for matched_path in matched_paths:
                if matched_path.startswith(path) and os.path.commonpath([path, matched_path]) == path:
                    additional_matched_paths[path] = st

        # Replace the snapshot's path dictionary with the newly filtered set.
        snapshot._stat_info = {**matched_paths, **additional_matched_paths}

        return snapshot

    async def _first_sync_sanity_checks(self, snapshot: DirectorySnapshot, diff: SnapshotDiff) -> SnapshotDiff:
        """Performs sanity checks to help the user from making syncing mistakes.

        One of the checks this performs is looking for unmatched deletes.  These are files/directories that will
        be deleted that don't match the current set of filters.  The implication here is that the only reason they
        are being deleted is because of a change in the filters.  For example, suppose a user syncs over the "foo"
        directory with a "-i foo" option.  Then they change the filter to sync the "bar" directory with "-i bar".
        This second sync would result in the "foo" directory being deleted in the remote.  This may or may not be
        what the user wants, so we ask what they want to do.

        The other check performed is to see whether there are any files matched by the current set of filters.
        If there are no files matched then it might mean that the user made a mistake in the filters.  For example,
        suppose they wanted all the Python files under "foo", but instead of `-ip "foo/*.py"` they wrote
        `-ip "fo/*.py"`.

        Args:
            snapshot (DirectorySnapshot): the current snapshot of the files/directories in the source
            diff (SnapshotDiff): diff comparing the current snapshot with the last snapshot from the previous run

        Returns:
            SnapshotDiff: the new diff to use, which has been updated based on the user's choice of how to handle
                          unmatched deletes
        """
        dbx_echo("Checking if any unmatched files/directories would be deleted")
        unmatched_delete_op_count = await self._dryrun_snapshot_diff_unmatched_deletes(diff)

        if not snapshot.paths:
            dbx_echo(
                "WARNING: No files were found under the source directory with the current filters. "
                "This means no files will be copied over."
            )
            dbx_echo(
                "WARNING: Consider adjusting the patterns you are filtering on "
                "(--include-pattern and --exclude-pattern)."
            )

        if unmatched_delete_op_count:
            dbx_echo(
                f"Detected {unmatched_delete_op_count} files and/or directories that will be deleted in the remote "
                "location because they don't match the current include/exclude filters."
            )

            delete_unmatched_option = self.delete_unmatched_option

            if delete_unmatched_option == DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED:
                dbx_echo("You most likely have changed the include/exclude filters since the last run.")
                dbx_echo("You can either:")
                dbx_echo("1) proceed with deleting these files in the remote location, or")
                dbx_echo("2) clear the paths from the local sync state so they won't be removed")
                dbx_echo("Note: see options --allow-delete-unmatched and --disallow-delete-unmatched")

                if click.confirm("Do you want to delete the files and directories above in the remote location?"):
                    delete_unmatched_option = DeleteUnmatchedOption.ALLOW_DELETE_UNMATCHED
                else:
                    delete_unmatched_option = DeleteUnmatchedOption.DISALLOW_DELETE_UNMATCHED

            if delete_unmatched_option == DeleteUnmatchedOption.ALLOW_DELETE_UNMATCHED:
                dbx_echo("Unmatched files/directories will be removed in the remote location.")
            else:
                # Update the diff to remove the unmatched file/directory deletes so they aren't deleted.
                dbx_echo("Unmatched files/directories will not be removed in the remote location.")
                diff = self._remove_unmatched_deletes(diff)

        return diff

    async def incremental_copy(self) -> int:
        """
        Performs an incremental copy from source using the client.

        Returns the number of operations performed or would have been performed (if dry run).
        """

        if self.is_first_sync:
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

        snapshot = self._prepare_snapshot()

        diff = compute_snapshot_diff(ref=self.last_snapshot, snapshot=snapshot)

        if self.is_first_sync:
            diff = await self._first_sync_sanity_checks(snapshot, diff)

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

        self.is_first_sync = False

        return op_count
