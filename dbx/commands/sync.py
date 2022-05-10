import asyncio
import os
import time
from getpass import getuser
from typing import List

import click

from dbx.sync.clients import BaseClient, DBFSClient, ReposClient
from dbx.sync.config import get_databricks_config
from dbx.sync.event_handler import file_watcher
from dbx.sync.path_matcher import PathMatcher
from dbx.sync import DeleteUnmatchedOption, RemoteSyncer
from dbx.utils import dbx_echo

# Patterns for files that are ignored by default.  There don't seem to be any reasonable scenarios where someone
# would want to sync these, so we don't make this configurable.
DEFAULT_IGNORES = [".git/", ".dbx", "*.isorted"]


def validate_allow_unmatched(ctx, param, value):  # noqa
    if value is None:
        return DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED
    if value:
        return DeleteUnmatchedOption.ALLOW_DELETE_UNMATCHED
    return DeleteUnmatchedOption.DISALLOW_DELETE_UNMATCHED


@click.group()
def cli():
    """
    Sync local files to Databricks and watch for changes.
    """
    pass


def create_path_matcher(*, source: str, includes: List[str], excludes: List[str]) -> PathMatcher:
    """Set up a pattern matcher that is used to ignores changes to files we don't want synced.

    Args:
        source (str): root directory includes and excludes are relative to
        includes (List[str]): Patterns to include.
        excludes (List[str]): Patterns to exclude.

    Returns:
        PathMatcher: matcher for matching files
    """

    ignores = list(DEFAULT_IGNORES)

    gitignore_path = os.path.join(source, ".gitignore")
    if os.path.exists(gitignore_path):
        dbx_echo(f"Ignoring patterns from {gitignore_path}")
        with open(gitignore_path, encoding="utf-8") as f:
            ignores.extend(f.readlines())

    if not includes:
        includes = []
        syncinclude_path = os.path.join(source, ".syncinclude")
        if os.path.exists(syncinclude_path):
            dbx_echo(f"Including patterns from {syncinclude_path}")
            with open(syncinclude_path, encoding="utf-8") as f:
                includes.extend(f.readlines())

    if excludes:
        ignores.extend(excludes)

    return PathMatcher(root_dir=source, ignores=ignores, includes=includes)


class StopIncrementalCopy(Exception):
    pass


def main_loop(
    *,
    source: str,
    client: BaseClient,
    full_sync: bool,
    dry_run: bool,
    includes: List[str],
    excludes: List[str],
    watch: bool,
    sleep_interval: float = 0.25,
    delete_unmatched_option: DeleteUnmatchedOption = DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED,
):
    """
    Performs the initial sync from the source directory and then watches for changes, performing
    an incremental sync whenever changes are detected.
    """

    matcher = create_path_matcher(source=source, includes=includes, excludes=excludes)

    syncer = RemoteSyncer(
        client=client,
        source=source,
        includes=includes,
        excludes=excludes,
        dry_run=dry_run,
        full_sync=full_sync,
        matcher=matcher,
        delete_unmatched_option=delete_unmatched_option,
    )

    dbx_echo("Starting initial copy")

    # Run the incremental copy and record how many operations were performed or would have been
    # performed (if in dry run mode).  An operation usually translates to an API call, such as
    # create a directory, put a file, etc.
    op_count = asyncio.run(syncer.incremental_copy())

    if not op_count:
        dbx_echo("No changes found during initial copy")

    if dry_run:
        dbx_echo("This was a dry run.  Exiting now.")
    elif watch:
        dbx_echo("Done. Watching for changes...")

        with file_watcher(source=source, matcher=matcher) as event_handler:
            while True:
                while True:
                    events = event_handler.get_events()
                    if events:
                        break
                    time.sleep(sleep_interval)

                try:
                    op_count = asyncio.run(syncer.incremental_copy())
                except StopIncrementalCopy:
                    # simple way to enable unit testing to break out of loop
                    break

                if op_count > 0:
                    dbx_echo("Done")


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
        full_subdir = os.path.join(source, subdir)
        if not os.path.exists(full_subdir):
            raise click.BadArgumentUsage(f"Path {full_subdir} does not exist")
        subdir = subdir.rstrip("/")
        patterns.append(f"/{subdir}/")
    return patterns


@cli.command()
@click.option("--profile", type=str, help="The databricks-cli profile to use for accessing the API token")
def check(profile: str):
    """
    Check if configuration for Databricks is valid.
    """
    # This will fail if the API token doesn't work.
    get_databricks_config(profile)

    click.echo(click.style("Databricks API token is properly configured and working", fg="green"))


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

    source = os.path.abspath(source).rstrip("/")

    dbx_echo(f"Syncing from {source}")

    return source


def common_options(f):
    f = click.option("--profile", type=str, help="The databricks-cli profile to use for accessing the API token")(f)
    f = click.option("--source", "-s", type=click.Path(exists=True, dir_okay=True, file_okay=False), required=False)(f)
    f = click.option("--full-sync", is_flag=True, help="Copy everything from the source on the initial sync")(f)
    f = click.option("--dry-run", is_flag=True)(f)
    f = click.option(
        "--include",
        "-i",
        "include_dirs",
        multiple=True,
        type=str,
        required=False,
        help="Directory under the source directory to sync",
    )(f)
    f = click.option("--exclude", "-e", "exclude_dirs", type=str, multiple=True)(f)
    f = click.option(
        "--include-pattern",
        "-ip",
        "include_patterns",
        multiple=True,
        type=str,
        required=False,
        help="Pattern under the source directory to sync",
    )(f)
    f = click.option(
        "--allow-delete-unmatched/--disallow-delete-unmatched",
        "delete_unmatched_option",
        callback=validate_allow_unmatched,
        type=bool,
        default=None,
        help="How to handle files/directories that would be deleted in remote because they don't match a filter",
    )(f)
    f = click.option("--exclude-pattern", "-ep", "exclude_patterns", type=str, multiple=True)(f)
    f = click.option("--watch/--no-watch", is_flag=True, default=True)(f)
    return f


@cli.command()
@common_options
@click.option("--dest", "-d", "dest_path", type=str, required=False)
@click.option("--user", "-u", "user_name", type=str)
def dbfs(
    user_name: str,
    source: str,
    full_sync: bool,
    dry_run: bool,
    include_dirs: List[str],
    dest_path: str,
    exclude_dirs: List[str],
    profile: str,
    watch: bool,
    include_patterns: List[str],
    exclude_patterns: List[str],
    delete_unmatched_option: DeleteUnmatchedOption,
):
    """
    Syncs from source directory to DBFS.

    By default this syncs to a destination path under /tmp/users/<username>.  The destination path is derived
    from the base name of the source path.  So syncing from /foo/bar would use a full destination path of
    /tmp/users/<username>/bar.  The destination path can be changed from the default base name using --dest.
    """

    # watch defaults to true, so to make it easy to just add --dry-run without having to add --no-watch,
    # we'll set watch to false here.
    if dry_run:
        watch = False

    config = get_databricks_config(profile)

    source = handle_source(source)

    include_patterns = list(include_patterns) or []
    exclude_patterns = list(exclude_patterns) or []

    include_patterns.extend(subdirs_to_patterns(source, include_dirs))
    exclude_patterns.extend(subdirs_to_patterns(source, exclude_dirs))

    # To make the tool easier to use, pick a reasonable destination path under /tmp if one is not specified that is
    # highly unlikely to overwrite anything important.
    if not dest_path:
        source_base_name = os.path.basename(source)
        if not source_base_name:
            raise click.UsageError("Destination path can't be determined.  Please specify with --dest.")

        if not user_name:
            user_name = getuser()

        if not user_name:
            raise click.UsageError(
                "Destination path can't be automatically determined because the user is not known. "
                "Please either specify the user with --user or provide the destination path with --dest."
            )

        dest_path = f"/tmp/users/{user_name}/{source_base_name}"

    # Syncing to root is probably a bad idea.
    if dest_path == "/":
        raise click.BadParameter("Destination cannot be the root path.  Please specify a subdirectory.")

    client = DBFSClient(base_path=dest_path, config=config)

    main_loop(
        source=source,
        client=client,
        full_sync=full_sync,
        dry_run=dry_run,
        includes=include_patterns,
        excludes=exclude_patterns,
        watch=watch,
        delete_unmatched_option=delete_unmatched_option,
    )


@cli.command()
@common_options
@click.option("--dest-repo", "-d", type=str, help="REPO in the destination path /Repos/USER/REPO", required=True)
@click.option(
    "--user",
    "-u",
    "user",
    type=str,
    help="USER in the destination path /Repos/USER/REPO",
    default=os.environ.get("DBX_SYNC_REPO_USER"),
)
def repo(
    user: str,
    source: str,
    full_sync: bool,
    dry_run: bool,
    include_dirs: List[str],
    dest_repo: str,
    exclude_dirs: List[str],
    profile: str,
    watch: bool,
    include_patterns: List[str],
    exclude_patterns: List[str],
    delete_unmatched_option: DeleteUnmatchedOption,
):
    """
    Syncs from source directory to a repo in Databricks.

    This can sync to a destination path under /Repos/<user>/<repo> using --dest-repo and --user.
    """

    if not user:
        raise click.UsageError("Repo user must be specified with --user")

    # watch defaults to true, so to make it easy to just add --dry-run without having to add --no-watch,
    # we'll set watch to false here.
    if dry_run:
        watch = False

    config = get_databricks_config(profile)

    source = handle_source(source)

    include_patterns = list(include_patterns) or []
    exclude_patterns = list(exclude_patterns) or []

    include_patterns.extend(subdirs_to_patterns(source, include_dirs))
    exclude_patterns.extend(subdirs_to_patterns(source, exclude_dirs))

    client = ReposClient(user=user, repo_name=dest_repo, config=config)

    main_loop(
        source=source,
        client=client,
        full_sync=full_sync,
        dry_run=dry_run,
        includes=include_patterns,
        excludes=exclude_patterns,
        watch=watch,
        delete_unmatched_option=delete_unmatched_option,
    )
