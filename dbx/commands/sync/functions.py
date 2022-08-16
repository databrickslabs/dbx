import os
import time
from pathlib import Path
from typing import List

import click
from databricks_cli.configure.provider import DatabricksConfig

from dbx.constants import DBX_SYNC_DEFAULT_IGNORES
from dbx.sync import DeleteUnmatchedOption, PathMatcher, BaseClient, RemoteSyncer
from dbx.sync.clients import get_user
from dbx.sync.event_handler import file_watcher
from dbx.utils import dbx_echo


def create_path_matcher(
    *,
    source: str,
    include_dirs: List[str] = None,
    exclude_dirs: List[str] = None,
    include_patterns: List[str] = None,
    exclude_patterns: List[str] = None,
    force_include_dirs: List[str] = None,
    force_include_patterns: List[str] = None,
    use_gitignore: bool = True,
) -> PathMatcher:
    """Set up a pattern matcher that is used to ignores changes to files we don't want synced.

    Args:
        source (str): root directory includes and excludes are relative to
        include_dirs (List[str]): Directories to include.
        exclude_dirs (List[str]): Directories to exclude.
        include_patterns (List[str]): Patterns to include.
        exclude_patterns (List[str]): Patterns to exclude.
        force_include_dirs (List[str]): Directories to include, even if they would otherwise be ignored due to
                                        exclude dirs/patterns.
        force_include_patterns (List[str]): Patterns to include, even if they would otherwise be ignored due to
                                        exclude dirs/patterns.
        use_gitignore (bool): Whether to use a .gitignore file, if it exists, to populate the list of exclude patterns,
                              in addition to any other exclude patterns that may be provided.

    Returns:
        PathMatcher: matcher for matching files
    """

    include_dirs = list(include_dirs) if include_dirs else []
    exclude_dirs = list(exclude_dirs) if exclude_dirs else []
    include_patterns = list(include_patterns) if include_patterns else []
    exclude_patterns = list(exclude_patterns) if exclude_patterns else []
    force_include_dirs = list(force_include_dirs) if force_include_dirs else []
    force_include_patterns = list(force_include_patterns) if force_include_patterns else []

    include_patterns.extend(subdirs_to_patterns(source, include_dirs))
    exclude_patterns.extend(subdirs_to_patterns(source, exclude_dirs))
    force_include_patterns.extend(subdirs_to_patterns(source, force_include_dirs))

    gitignore_path = Path(source) / ".gitignore"
    if use_gitignore and gitignore_path.exists():
        dbx_echo(f"Ignoring patterns from {gitignore_path}")
        exclude_patterns.extend(gitignore_path.read_text(encoding="utf-8").splitlines())

    syncinclude_path = Path(source) / ".syncinclude"
    if not include_patterns and syncinclude_path.exists():
        dbx_echo(f"Including patterns from {syncinclude_path}")
        include_patterns.extend(syncinclude_path.read_text(encoding="utf-8").splitlines())

    exclude_patterns.extend(DBX_SYNC_DEFAULT_IGNORES)

    return PathMatcher(
        root_dir=source, ignores=exclude_patterns, includes=include_patterns, force_includes=force_include_patterns
    )


def subdirs_to_patterns(source: str, subdirs: List[str]) -> List[str]:
    """Converts a list of subdirectories under a source directory to a
    list of gitignore-style patterns that match those directories.

    Args:
        source (str): source directory these subdirs exist under
        subdirs (List[str]): subdirectories to create patterns for

    Raises:
        click.BadArgumentUsage: directory doesn't exist

    Returns:
        List[str]: list of patterns matching subdirs
    """
    patterns = []
    for subdir in subdirs:
        full_subdir = Path(source) / subdir
        if not full_subdir.exists():
            raise click.BadArgumentUsage(f"Path {full_subdir} does not exist")
        subdir = Path(subdir).as_posix()
        patterns.append(f"/{subdir}/")
    return patterns


def main_loop(
    *,
    source: str,
    matcher: PathMatcher,
    client: BaseClient,
    full_sync: bool,
    dry_run: bool,
    watch: bool,
    sleep_interval: float = 0.25,
    polling_interval_secs: float = None,
    delete_unmatched_option: DeleteUnmatchedOption = DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED,
):
    """
    Performs the initial sync from the source directory and then watches for changes, performing
    an incremental sync whenever changes are detected.
    """

    syncer = RemoteSyncer(
        client=client,
        source=source,
        dry_run=dry_run,
        full_sync=full_sync,
        matcher=matcher,
        delete_unmatched_option=delete_unmatched_option,
    )

    dbx_echo("Starting initial copy")

    # Run the incremental copy and record how many operations were performed or would have been
    # performed (if in dry run mode).  An operation usually translates to an API call, such as
    # create a directory, put a file, etc.
    op_count = syncer.incremental_copy()

    if not op_count:
        dbx_echo("No changes found during initial copy")

    if dry_run:
        dbx_echo("This was a dry run.  Exiting now.")
    elif watch:
        dbx_echo("Done. Watching for changes...")

        with file_watcher(source=source, matcher=matcher, polling_interval_secs=polling_interval_secs) as event_handler:
            while True:
                # Keep looping until the event handler sees some file system events
                # under the source path that match the provided filters.
                while True:
                    events = event_handler.get_events()

                    # Once at least one event has occurred, break out of the loop so we can
                    # sync the change over.
                    if events:
                        break
                    time.sleep(sleep_interval)

                # Run incremental copy to sync over changes since the last sync.
                op_count = syncer.incremental_copy()

                # simple way to enable unit testing to break out of loop
                if op_count < 0:
                    break

                dbx_echo("Done")


def handle_source(source: str = None) -> str:
    """Determine the source directory to sync from.  If the source directory is not specified
    then it will check if the current directory has a .git subdirectory, implying that the current
    directory should be the source.

    Args:
        source (str): source directory to use, or None to try automatically determining

    Raises:
        click.UsageError: When the source directory is unspecified and can't be automatically determined.

    Returns:
        str: source directory to sync from
    """
    if not source:
        # If this is being run from the base of a git repo, then they probably want to use
        # the repo as the source.
        if os.path.exists(".git"):
            source = "."
        else:
            raise click.UsageError("Must specify source directory using --source")

    source = os.path.abspath(source)

    dbx_echo(f"Syncing from {source}")

    return source


def get_user_name(config: DatabricksConfig) -> str:
    """Gets the name of the user according to the Databricks API using the config for authorization.

    Args:
        config (DatabricksConfig): config to use to get user info

    Returns:
        str: name of user
    """
    user_info = get_user(config)
    return user_info.get("userName")


def get_source_base_name(source: str) -> str:
    source_base_name = os.path.basename(source.rstrip("/"))
    if not source_base_name:
        raise click.UsageError("Destination path can't be determined.  Please specify with --dest.")
    return source_base_name
