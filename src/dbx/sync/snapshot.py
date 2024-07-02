from dataclasses import dataclass
from typing import List

from watchdog.utils.dirsnapshot import DirectorySnapshot


@dataclass
class SnapshotDiff:
    files_created: List[str]
    files_deleted: List[str]
    files_modified: List[str]
    dirs_created: List[str]
    dirs_deleted: List[str]


def compute_snapshot_diff(ref: DirectorySnapshot, snapshot: DirectorySnapshot) -> SnapshotDiff:
    """Computes a diff between two directory snapshots at different points in time.

    See SnapshotDiff for what changes this detects.  This ignores changes to attributes of directories,
    such as the modification time.  It also does not attempt to detect moves.  A move is represented
    as a delete and create.

    This diff is useful for applying changes to a remote blob storage system that supports CRUD operations.
    Typically these systems don't support move operations, which is why detecting moves is not a priority.

    Args:
        ref (DirectorySnapshot): what we're comparing against (the old snapshot)
        snapshot (DirectorySnapshot): the new snapshot

    Returns:
        SnapshotDiff: record of what has changed between the snapshots
    """
    created = snapshot.paths - ref.paths
    deleted = ref.paths - snapshot.paths

    # any paths that exist in both ref and snapshot may have been modified, so we'll need to do some checks.
    modified_candidates = ref.paths & snapshot.paths

    # check if file was replaced with a dir, or dir was replaced with a file
    for path in modified_candidates:
        snapshot_is_dir = snapshot.isdir(path)
        ref_is_dir = ref.isdir(path)
        if (snapshot_is_dir and not ref_is_dir) or (not snapshot_is_dir and ref_is_dir):
            created.add(path)
            deleted.add(path)

    # anything created or deleted is by definition not modified
    modified_candidates = modified_candidates - created - deleted

    # check if any of remaining candidates were modified based on size or mtime changes
    modified = set()
    for path in modified_candidates:
        if ref.mtime(path) != snapshot.mtime(path) or ref.size(path) != snapshot.size(path):
            modified.add(path)

    dirs_created = sorted([path for path in created if snapshot.isdir(path)])
    dirs_deleted = sorted([path for path in deleted if ref.isdir(path)])
    dirs_modified = sorted([path for path in modified if ref.isdir(path)])

    files_created = sorted(list(created - set(dirs_created)))
    files_deleted = sorted(list(deleted - set(dirs_deleted)))
    files_modified = sorted(list(modified - set(dirs_modified)))

    return SnapshotDiff(
        files_created=files_created,
        files_deleted=files_deleted,
        files_modified=files_modified,
        dirs_created=dirs_created,
        dirs_deleted=dirs_deleted,
    )
