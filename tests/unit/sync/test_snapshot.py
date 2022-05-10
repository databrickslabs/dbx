import os
import shutil
from pathlib import Path

from watchdog.utils.dirsnapshot import DirectorySnapshot

from dbx.sync.snapshot import compute_snapshot_diff
from dbx.sync import get_relative_path

from .utils import temporary_directory


def test_empty_dir():
    with temporary_directory() as source:

        # initially no files
        snapshot1 = DirectorySnapshot(source)

        # still no files
        snapshot2 = DirectorySnapshot(source)

        diff = compute_snapshot_diff(ref=snapshot1, snapshot=snapshot2)

        assert len(diff.dirs_created) == 0
        assert len(diff.dirs_deleted) == 0
        assert len(diff.files_created) == 0
        assert len(diff.files_deleted) == 0
        assert len(diff.files_modified) == 0


def test_create_dir():
    with temporary_directory() as source:

        snapshot1 = DirectorySnapshot(source)

        (Path(source) / "foo").mkdir()

        snapshot2 = DirectorySnapshot(source)

        diff = compute_snapshot_diff(ref=snapshot1, snapshot=snapshot2)

        assert len(diff.dirs_created) == 1
        assert len(diff.dirs_deleted) == 0
        assert len(diff.files_created) == 0
        assert len(diff.files_deleted) == 0
        assert len(diff.files_modified) == 0

        assert get_relative_path(source, diff.dirs_created[0]) == "foo"


def test_delete_dir():
    with temporary_directory() as source:

        (Path(source) / "foo").mkdir()

        snapshot1 = DirectorySnapshot(source)

        os.rmdir(Path(source) / "foo")

        snapshot2 = DirectorySnapshot(source)

        diff = compute_snapshot_diff(ref=snapshot1, snapshot=snapshot2)

        assert len(diff.dirs_created) == 0
        assert len(diff.dirs_deleted) == 1
        assert len(diff.files_created) == 0
        assert len(diff.files_deleted) == 0
        assert len(diff.files_modified) == 0

        assert get_relative_path(source, diff.dirs_deleted[0]) == "foo"


def test_move_dir():
    with temporary_directory() as source:

        (Path(source) / "foo").mkdir()

        snapshot1 = DirectorySnapshot(source)

        os.rename((Path(source) / "foo"), (Path(source) / "foo2"))

        snapshot2 = DirectorySnapshot(source)

        diff = compute_snapshot_diff(ref=snapshot1, snapshot=snapshot2)

        assert len(diff.dirs_created) == 1
        assert len(diff.dirs_deleted) == 1
        assert len(diff.files_created) == 0
        assert len(diff.files_deleted) == 0
        assert len(diff.files_modified) == 0

        assert get_relative_path(source, diff.dirs_deleted[0]) == "foo"
        assert get_relative_path(source, diff.dirs_created[0]) == "foo2"


def test_create_file():
    with temporary_directory() as source:

        snapshot1 = DirectorySnapshot(source)

        (Path(source) / "foo").touch()

        snapshot2 = DirectorySnapshot(source)

        diff = compute_snapshot_diff(ref=snapshot1, snapshot=snapshot2)

        assert len(diff.dirs_created) == 0
        assert len(diff.dirs_deleted) == 0
        assert len(diff.files_created) == 1
        assert len(diff.files_deleted) == 0
        assert len(diff.files_modified) == 0

        assert get_relative_path(source, diff.files_created[0]) == "foo"


def test_delete_file():
    with temporary_directory() as source:

        (Path(source) / "foo").touch()

        snapshot1 = DirectorySnapshot(source)

        os.remove(Path(source) / "foo")

        snapshot2 = DirectorySnapshot(source)

        diff = compute_snapshot_diff(ref=snapshot1, snapshot=snapshot2)

        assert len(diff.dirs_created) == 0
        assert len(diff.dirs_deleted) == 0
        assert len(diff.files_created) == 0
        assert len(diff.files_deleted) == 1
        assert len(diff.files_modified) == 0

        assert get_relative_path(source, diff.files_deleted[0]) == "foo"


def test_modify_file():
    with temporary_directory() as source:

        (Path(source) / "foo").touch()

        snapshot1 = DirectorySnapshot(source)

        with open(Path(source) / "foo", "w") as f:
            f.write("blah")

        snapshot2 = DirectorySnapshot(source)

        diff = compute_snapshot_diff(ref=snapshot1, snapshot=snapshot2)

        assert len(diff.dirs_created) == 0
        assert len(diff.dirs_deleted) == 0
        assert len(diff.files_created) == 0
        assert len(diff.files_deleted) == 0
        assert len(diff.files_modified) == 1

        assert get_relative_path(source, diff.files_modified[0]) == "foo"


def test_move_file():
    with temporary_directory() as source:

        (Path(source) / "foo").touch()

        snapshot1 = DirectorySnapshot(source)

        os.rename((Path(source) / "foo"), (Path(source) / "foo2"))

        snapshot2 = DirectorySnapshot(source)

        diff = compute_snapshot_diff(ref=snapshot1, snapshot=snapshot2)

        assert len(diff.dirs_created) == 0
        assert len(diff.dirs_deleted) == 0
        assert len(diff.files_created) == 1
        assert len(diff.files_deleted) == 1
        assert len(diff.files_modified) == 0

        assert get_relative_path(source, diff.files_deleted[0]) == "foo"
        assert get_relative_path(source, diff.files_created[0]) == "foo2"


def test_create_dir_with_file():
    with temporary_directory() as source:

        snapshot1 = DirectorySnapshot(source)

        (Path(source) / "foo").mkdir()
        (Path(source) / "foo" / "bar").touch()

        snapshot2 = DirectorySnapshot(source)

        diff = compute_snapshot_diff(ref=snapshot1, snapshot=snapshot2)

        assert len(diff.dirs_created) == 1
        assert len(diff.dirs_deleted) == 0
        assert len(diff.files_created) == 1
        assert len(diff.files_deleted) == 0
        assert len(diff.files_modified) == 0

        assert get_relative_path(source, diff.dirs_created[0]) == "foo"
        assert Path(get_relative_path(source, diff.files_created[0])).as_posix() == "foo/bar"


def test_move_dir_with_file():
    with temporary_directory() as source:

        (Path(source) / "foo").mkdir()
        (Path(source) / "foo" / "bar").touch()

        snapshot1 = DirectorySnapshot(source)

        os.rename((Path(source) / "foo"), (Path(source) / "foo2"))

        snapshot2 = DirectorySnapshot(source)

        diff = compute_snapshot_diff(ref=snapshot1, snapshot=snapshot2)

        assert len(diff.dirs_created) == 1
        assert len(diff.dirs_deleted) == 1
        assert len(diff.files_created) == 1
        assert len(diff.files_deleted) == 1
        assert len(diff.files_modified) == 0

        assert get_relative_path(source, diff.dirs_deleted[0]) == "foo"
        assert get_relative_path(source, diff.dirs_created[0]) == "foo2"
        assert Path(get_relative_path(source, diff.files_deleted[0])).as_posix() == "foo/bar"
        assert Path(get_relative_path(source, diff.files_created[0])).as_posix() == "foo2/bar"


def test_delete_dir_with_file():
    with temporary_directory() as source:

        (Path(source) / "foo").mkdir()
        (Path(source) / "foo" / "bar").touch()

        snapshot1 = DirectorySnapshot(source)

        shutil.rmtree(Path(source) / "foo")

        snapshot2 = DirectorySnapshot(source)

        diff = compute_snapshot_diff(ref=snapshot1, snapshot=snapshot2)

        assert len(diff.dirs_created) == 0
        assert len(diff.dirs_deleted) == 1
        assert len(diff.files_created) == 0
        assert len(diff.files_deleted) == 1
        assert len(diff.files_modified) == 0

        assert get_relative_path(source, diff.dirs_deleted[0]) == "foo"
        assert Path(get_relative_path(source, diff.files_deleted[0])).as_posix() == "foo/bar"


def test_replace_dir_with_file():
    with temporary_directory() as source:

        (Path(source) / "foo").mkdir()

        snapshot1 = DirectorySnapshot(source)

        os.rmdir(Path(source) / "foo")
        (Path(source) / "foo").touch()

        snapshot2 = DirectorySnapshot(source)

        diff = compute_snapshot_diff(ref=snapshot1, snapshot=snapshot2)

        assert len(diff.dirs_created) == 0
        assert len(diff.dirs_deleted) == 1
        assert len(diff.files_created) == 1
        assert len(diff.files_deleted) == 0
        assert len(diff.files_modified) == 0

        assert get_relative_path(source, diff.dirs_deleted[0]) == "foo"
        assert get_relative_path(source, diff.files_created[0]) == "foo"


def test_replace_file_with_dir():
    with temporary_directory() as source:

        (Path(source) / "foo").touch()

        snapshot1 = DirectorySnapshot(source)

        os.remove(Path(source) / "foo")
        (Path(source) / "foo").mkdir()

        snapshot2 = DirectorySnapshot(source)

        diff = compute_snapshot_diff(ref=snapshot1, snapshot=snapshot2)

        assert len(diff.dirs_created) == 1
        assert len(diff.dirs_deleted) == 0
        assert len(diff.files_created) == 0
        assert len(diff.files_deleted) == 1
        assert len(diff.files_modified) == 0

        assert get_relative_path(source, diff.dirs_created[0]) == "foo"
        assert get_relative_path(source, diff.files_deleted[0]) == "foo"
