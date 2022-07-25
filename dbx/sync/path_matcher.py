import os
from pathlib import Path
from typing import List, Union

import pathspec


def path_as_posix(path: Union[str, Path]) -> str:
    if isinstance(path, str):
        ends_with_sep = path.endswith("/") or path.endswith("\\")
        path = Path(path).as_posix()
        if ends_with_sep:
            path = path + "/"
    else:
        path = path.as_posix()
    return path


class PathMatcher:
    """
    Applies gitignore-style ignore and include patterns to match file paths.

    Initialized with defaults for ignores and includes, this will match every path that is passed in, as long
    as it is under the root directory (otherwise it would be an error).

    Initialized with only ignores, this will match any path that does not match one of the ignore patterns.

    Initialized with only includes, this will only match paths that match one of the include patterns.

    Both ignores and includes can be specified together, where ignores take precedence.
    """

    def __init__(
        self,
        root_dir: Union[str, Path],
        *,
        ignores: List[str] = None,
        includes: List[str] = None,
        force_includes: List[str] = None,
    ):
        """Initialize

        Args:
            root_dir (str): the root directory that the ignore and include patterns are relative to
            ignores (List[str], optional): patterns to ignore. Defaults to None, which means nothing will be
                                           ignored by default.
            includes (List[str], optional): patterns to include. Defaults to None, which means everything will be
                                            included, unless otherwise ignored.
            force_includes (List[str], optional): patterns to include, even if they would otherwise be ignored.
                                                  Defaults to None, which means nothing will be forced to be included.
        """

        self.root_dir = path_as_posix(root_dir)
        self.includes = includes
        self.force_includes = force_includes
        self.ignores = ignores
        self.include_spec = pathspec.PathSpec.from_lines("gitwildmatch", includes) if includes else None
        self.force_include_spec = (
            pathspec.PathSpec.from_lines("gitwildmatch", force_includes) if force_includes else None
        )
        self.ignore_spec = pathspec.PathSpec.from_lines("gitwildmatch", ignores) if ignores else None

    def _clean_relative_path(self, path: str, *, is_directory: bool) -> str:
        # pathspec does not check if a path is a directory, so we need to tell it by adding a trailing slash.
        if not path.endswith("/"):
            # Note: it's possible that the path may no longer exist.  therefore the is_directory flag is useful in
            # this case.
            if is_directory or os.path.isdir(path):
                path = path + "/"
        elif is_directory is False:
            raise ValueError("Path should not end in '/' and also not be a directory")

        path = path[len(self.root_dir) + 1 :]

        return path

    def should_ignore(self, path: Union[str, Path], *, is_directory: bool = None) -> bool:
        """Check if the path should be ignored due to an ignore spec.

        Args:
            path (Union[str, Path]): path to check
            is_directory (bool, optional): True if this is a directory, False otherwise.  By default this is None,
                                           meaning it is unspecified.

        Returns:
            bool: True if it should be ignored due to the ignore spec
        """
        path = path_as_posix(path)

        path = self._clean_relative_path(path, is_directory=is_directory)

        # We certaintly will not ignore something that we've forced to be included.
        if self.force_include_spec is not None and self.force_include_spec.match_file(path):
            return False

        # If there is an ignore spec, then we'll ignore any paths that match.
        if self.ignore_spec is not None:
            return self.ignore_spec.match_file(path)

        return False

    def match(self, path: Union[str, Path], *, is_directory: bool = None) -> bool:
        """Check if the path should be matched.  This happens when either:

        1) A list of ignore patterns have been provided and the path matches one of those patterns.
        2) A list of include patterns has been provided and the path does not match one of those patterns.

        gitignore patterns distinguish between files and directories using a trailing slash.  For the path passed
        into this function, a directory can be indicated either by appending a slash or by setting is_directory
        to True.  If neither of these are used, then a slash is automatically appended if the path exists and
        is a directory.

        Args:
            path (str): the path to check
            is_directory (bool, optional): True if this is a directory, False otherwise.  By default this is None,
                                           meaning it is unspecified.

        Returns:
            bool: True if the path should be ignored, and False otherwise
        """

        path = path_as_posix(path)

        # Sometimes we get events that are outside of the root directory.  For example we may get a directory
        # creation event about the source directory, which also results in a modification event for the parent
        # of the source directory.
        if not path.startswith(self.root_dir + "/"):
            return False

        # This class only matches files within a source directory, but not that source directory itself.
        if path.rstrip("/") == self.root_dir:
            return False

        path = self._clean_relative_path(path, is_directory=is_directory)

        # If there is a force include spec, then we'll return True if it matches, even if it would have otherwise
        # been ignored below.
        if self.force_include_spec is not None and self.force_include_spec.match_file(path):
            return True

        # If there is an ignore spec, then we'll ignore any paths that match, regardless of the include spec.
        if self.ignore_spec is not None:
            should_ignore = self.ignore_spec.match_file(path)
            if should_ignore:
                return False

        # If there is an include spec, then we'll ignore any paths that don't match.  If there isn't an include
        # spec then we'll include everything by default.
        if self.include_spec is not None:
            return self.include_spec.match_file(path)

        return True


def filtered_listdir(matcher: PathMatcher, root: str):
    """A wrapper around os.scandir that filters out paths that should definitely be ignored according to rules in the
    given matcher.  Enabling this filtering makes the traversal more efficient.

    To, apply a partial to pass in the matcher:

        matcher = ...
        snapshot = DirectorySnapshot(self.source, listdir=partial(matcher, filtered_listdir))

    Args:
        matcher (PathMatcher): the matcher to use for filtering
        root (str): root path to list files/directories

    Yields:
        str: file/directory entries
    """
    for entry in os.scandir(root):
        entry_name = Path(root) / (entry if isinstance(entry, str) else entry.name)
        # Some paths are definitely ignored due to an ignore spec.  These should not be traversed.
        if not matcher.should_ignore(entry_name, is_directory=os.path.isdir(entry_name)):
            yield entry
