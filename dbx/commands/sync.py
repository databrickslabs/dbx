import os
import time
from pathlib import Path
from typing import List

import click
from databricks_cli.configure.provider import DatabricksConfig

from dbx.sync.clients import BaseClient, DBFSClient, ReposClient, get_user
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
def sync():
    """
    Sync local files to Databricks and watch for changes, with support for syncing to either a path
    in `DBFS <https://docs.databricks.com/data/databricks-file-system.html>`_ or a
    `Databricks Repo <https://docs.databricks.com/repos/index.html>`_ via the :code:`dbfs` and :code:`repo`
    subcommands.  This enables one to incrementally sync local files to Databricks in order to enable quick, iterative
    development in an IDE with the ability to test changes almost immediately in Databricks notebooks.

    Suppose you are using the `Repos for Git integration <https://docs.databricks.com/repos/index.html>`_ feature
    and have cloned a git repo within Databricks where you have Python notebooks stored as well as various Python
    modules that the notebooks import. You can edit any of these files directly in Databricks.
    The :code:`dbx sync repo` command provides an additional option: edit the files in a local repo on your computer
    in an IDE of your choice and sync the changes to the repo in Databricks as you make changes.

    For example, when run from a local git clone, the following will sync all the files to an existing repo
    named :code:`myrepo` in Databricks and watch for changes:

    .. code-block:: sh

        dbx sync repo -d myrepo

    At the top of your notebook you can turn on
    `autoreload <https://ipython.org/ipython-doc/3/config/extensions/autoreload.html>`_ so that execution of cells
    will automatically pick up the changes:

    .. code-block::

        %load_ext autoreload
        %autoreload 2

    The :code:`dbx sync repo` command syncs to a repo in Databricks. If that repo is a git clone you can see the
    changes made to the files, as if you'd made the edits directly in Databricks. Alternatively, you can use
    :code:`dbx sync dbfs` to sync the files to a path in DBFS. This keeps the files independent from the repos but
    still allows you to use them in notebooks either in a repo or in notebooks existing in your workspace.

    For example, when run from a local git clone in a :code:`myrepo` directory under a user
    :code:`first.last@somewhere.com`, the following will sync all the files to the DBFS path
    :code:`/tmp/users/first.last/myrepo`:

    .. code-block:: sh

        dbx sync dbfs

    The destination path can also be specified, as in: :code:`-d /tmp/myrepo`.

    When executing notebooks in a repo, the root of the repo is automatically added to the Python path so that
    imports work relative to the repo root. This means that aside from turning on autoreload you don't need to do
    anything else special for the changes to be reflected in the cell's execution. However, when syncing to DBFS,
    for the imports to work you need to update the Python path to include this target directory you're syncing to.
    For example, to import from the :code:`/tmp/users/first.last/myrepo` path used above, use the following at the top
    of your notebook:

    .. code-block:: python

        import sys

        if "/dbfs/tmp/users/first.last/myrepo" not in sys.path:
            sys.path.insert(0, "/dbfs/tmp/users/first.last/myrepo")

    The :code:`dbx sync` commands have many options for controlling which files/directories to include/exclude from
    syncing, which are well documented below.  For convenience, all patterns listed in a :code:`.gitignore` at the
    source will be excluded from syncing. The :code:`.git` directory is excluded as well.

    """
    pass  # pragma: no cover


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

    exclude_patterns.extend(DEFAULT_IGNORES)

    return PathMatcher(
        root_dir=source, ignores=exclude_patterns, includes=include_patterns, force_includes=force_include_patterns
    )


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


def common_options(f):
    f = click.option(
        "--profile",
        type=str,
        help="""The Databricks CLI
                `connection profile <https://docs.databricks.com/dev-tools/cli/index.html#connection-profiles>`_
                containing the host and API token to use to connect to Databricks.""",
    )(f)
    f = click.option(
        "--source",
        "-s",
        type=click.Path(exists=True, dir_okay=True, file_okay=False),
        required=False,
        help="""The local source path to sync from.  If the current working directory is a git repo,
        then the tool by default uses that path as the source.  Otherwise the source path will need
        to be specified.""",
    )(f)
    f = click.option(
        "--full-sync",
        is_flag=True,
        help="""Ignores any existing sync state and syncs all files and directories matching the filters to the
        destination.""",
    )(f)
    f = click.option("--dry-run", is_flag=True, help="Log what the tool would do without making any changes.")(f)
    f = click.option(
        "--include",
        "-i",
        "include_dirs",
        multiple=True,
        type=str,
        required=False,
        help="""A directory to sync, relative to the source directory.  This directory must exist.
        When this option is used, no files or directories will be synced unless specifically included
        by this or other include options.

        For example:

        * :code:`-i foo` will sync a directory `foo` directly under the source directory
        * :code:`-i foo/bar` will sync a directory `foo/bar` directly under the source directory
        """,
    )(f)
    f = click.option(
        "--force-include",
        "-fi",
        "force_include_dirs",
        multiple=True,
        type=str,
        required=False,
        help="""A directory to sync, relative to the source directory.  This directory must exist.
        When this option is used, no files or directories will be synced unless specifically included
        by this or other include options.

        Unlike `--include`, this will sync a directory regardless of files/directories that are excluded from
        syncing.  This can be useful when, for example, the `.gitignore` lists a directory that you want to have
        synced.  The patterns in the `.gitignore` are used by default to exclude files/directories from syncing.

        For example:

        * :code:`-i foo` will sync a directory `foo` directly under the source directory
        * :code:`-i foo/bar` will sync a directory `foo/bar` directly under the source directory
        """,
    )(f)
    f = click.option(
        "--exclude",
        "-e",
        "exclude_dirs",
        type=str,
        multiple=True,
        help="""A directory to exclude from syncing, relative to the source directory.  This directory must exist.

        For example:

        * :code:`-e foo` will exclude directory `foo` directly under the source directory from syncing
        * :code:`-e foo/bar` will exclude directory `foo/bar` directly under the source directory from syncing
        """,
    )(f)
    f = click.option(
        "--include-pattern",
        "-ip",
        "include_patterns",
        multiple=True,
        type=str,
        required=False,
        help="""A pattern specifying files and/or directories to sync, relative to the source directory.
        This uses the same format as `gitignore <https://git-scm.com/docs/gitignore>`_.
        When this option is used, no files or directories will be synced unless specifically included
        by this or other include options.

        For example:

        * :code:`foo` will match any file or directory named `foo` anywhere under the source
        * :code:`/foo/` will only match a directory named `foo` directly under the source.
        * :code:`*.py` will match all Python files.
        * :code:`/foo/*.py` will match all Python files directly under the `foo` directory.
        * :code:`/foo/**/*.py` will match all Python files anywhere under the `foo` directory.

        You may also store a list of patterns inside a :code:`.syncinclude` file under the source path.
        Patterns in this file will be used as the default patterns to include.  This essentially behaves
        as the opposite of a `gitignore <https://git-scm.com/docs/gitignore>`_ file, but with the same format.
        """,
    )(f)
    f = click.option(
        "--force-include-pattern",
        "-fip",
        "force_include_patterns",
        multiple=True,
        type=str,
        required=False,
        help="""A pattern specifying files and/or directories to sync, relative to the source directory, regardless
        of whether these files and/or directories would otherwise be excluded.

        See documentation of `--include-pattern` for usage.
        """,
    )(f)
    f = click.option(
        "--allow-delete-unmatched/--disallow-delete-unmatched",
        "delete_unmatched_option",
        callback=validate_allow_unmatched,
        type=bool,
        default=None,
        help="""Specifies how to handle files/directories that would be deleted in the remote destination because
        they don't match the current set of filters.

        For example, suppose you have used the option :code:`-i foo` to sync only the `foo` directory and later
        quit the tool.  Then suppose you restart the tool using :code:`-i bar` to sync only the `bar` directory.
        In this situation, it is unclear whether your intention is to 1) sync over `bar` and remove `foo` in the
        destination, or 2) sync over `bar` and leave `foo` alone in the destination.  Due to this ambiguity, the tool
        will ask to confirm your intentions.

        To avoid having to confirm, you can use either of these options:

        * :code:`--allow-delete-unmatched` will delete files/directories in the destination that are not present
          locally with the current filters.  So for the example above, this would remove `foo` in the destination when
          syncing with :code:`-i bar`.
        * :code:`--disallow-delete-unmatched` will NOT delete files/directories in the destination that are not present
          locally with the current filters.  So for the example above, this would leave `foo` in the destination when
          syncing with :code:`-i bar`.
        """,
    )(f)
    f = click.option(
        "--exclude-pattern",
        "-ep",
        "exclude_patterns",
        type=str,
        multiple=True,
        help="""A pattern specifying files and/or directories to exclude from syncing, relative to the source directory.
        This uses the same format as `gitignore <https://git-scm.com/docs/gitignore>`_.
        For examples, see the documentation of :code:`--include-pattern`.""",
    )(f)
    f = click.option(
        "--watch/--no-watch",
        is_flag=True,
        default=True,
        help="""Controls whether the tool should watch for file changes after the initial sync.
                     With :code:`--watch`, which is the default, it will watch for file system changes
                     and rerun the sync whenever any changes occur to files or directories matching the filters.
                     With :code:`--no-watch` the tool will quit after the initial sync.""",
    )(f)
    f = click.option(
        "--polling-interval",
        "polling_interval_secs",
        type=float,
        help="Use file system polling instead of file system events and set the polling interval (in seconds)",
    )(f)
    f = click.option(
        "--use-gitignore/--no-use-gitignore",
        is_flag=True,
        default=True,
        help="""Controls whether the .gitignore is used to automatically exclude file/directories from syncing.""",
    )(f)
    return f


@sync.command()
@common_options
@click.option(
    "--dest",
    "-d",
    "dest_path",
    type=str,
    required=False,
    help="""A path in DBFS to sync to.  For example, :code:`-d /tmp/project` would sync from the
    local source path to the DBFS path :code:`/tmp/project`.

    Specifying this path is optional.  By default the tool will sync to the destination
    :code:`/tmp/users/<user_name>/<source_base_name>`.  For example, given local source path :code:`/foo/bar`
    and Databricks user :code:`first.last@somewhere.com`, this would sync to :code:`/tmp/users/first.last/bar`.
    This path is chosen as a safe default option that is unlikely to overwrite anything important.

    When constructing this default destination path, the user name is determined using the
    `Databricks API <https://docs.databricks.com/dev-tools/api/latest/scim/scim-me.html>`_.  If it cannot be determined,
    or to use a different user for the path, you may use the :code:`--user` option.
    """,
)
@click.option(
    "--user",
    "-u",
    "user_name",
    type=str,
    help="""Specify the user name to use when constructing the default destination path.
    This has no effect when :code:`--dest` is already specified.  If this is an email address then the domain is
    ignored. For example :code:`-u first.last` and :code:`-u first.last@somewhere.com` will both result
    in :code:`first.last` as the user name.""",
)
def dbfs(
    user_name: str,
    source: str,
    full_sync: bool,
    dry_run: bool,
    include_dirs: List[str],
    force_include_dirs: List[str],
    dest_path: str,
    exclude_dirs: List[str],
    profile: str,
    watch: bool,
    polling_interval_secs: float,
    include_patterns: List[str],
    force_include_patterns: List[str],
    exclude_patterns: List[str],
    use_gitignore: bool,
    delete_unmatched_option: DeleteUnmatchedOption,
):
    """
    Syncs from a source directory to `DBFS <https://docs.databricks.com/data/databricks-file-system.html>`_.
    """

    # watch defaults to true, so to make it easy to just add --dry-run without having to add --no-watch,
    # we'll set watch to false here.
    if dry_run:
        watch = False

    config = get_databricks_config(profile)

    source = handle_source(source)

    matcher = create_path_matcher(
        source=source,
        include_dirs=include_dirs,
        exclude_dirs=exclude_dirs,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
        use_gitignore=use_gitignore,
        force_include_dirs=force_include_dirs,
        force_include_patterns=force_include_patterns,
    )

    # To make the tool easier to use, pick a reasonable destination path under /tmp if one is not specified that is
    # highly unlikely to overwrite anything important.
    if not dest_path:
        source_base_name = get_source_base_name(source)

        if not user_name:
            user_name = get_user_name(config)

        if not user_name:
            raise click.UsageError(
                "Destination path can't be automatically determined because the user is not known. "
                "Please either specify the user with --user or provide the destination path with --dest."
            )

        # if user name is an email, just use the first part
        user_name = user_name.split("@")[0]

        dest_path = f"/tmp/users/{user_name}/{source_base_name}"

    # Syncing to root is probably a bad idea.
    if dest_path == "/":
        raise click.BadParameter("Destination cannot be the root path.  Please specify a subdirectory.")

    client = DBFSClient(base_path=dest_path, config=config)

    main_loop(
        source=source,
        matcher=matcher,
        client=client,
        full_sync=full_sync,
        dry_run=dry_run,
        watch=watch,
        polling_interval_secs=polling_interval_secs,
        delete_unmatched_option=delete_unmatched_option,
    )


@sync.command()
@common_options
@click.option(
    "--dest-repo",
    "-d",
    type=str,
    help="""The name of the `Databricks Repo <https://docs.databricks.com/repos/index.html>`_ to sync to.

    Repos exist in the Databricks workspace under a path  of the form :code:`/Repos/<user>/<repo>`.  This specifies the
    :code:`<repo>` portion of the path.""",
    required=True,
)
@click.option(
    "--user",
    "-u",
    "user_name",
    type=str,
    help="""The user who owns the `Databricks Repo <https://docs.databricks.com/repos/index.html>`_ to sync to.

    Repos exist in the Databricks workspace under a path  of the form :code:`/Repos/<user>/<repo>`.  This specifies the
    :code:`<user>` portion of the path.

    This is optional, as the user name is determined automatically using the
    `Databricks API <https://docs.databricks.com/dev-tools/api/latest/scim/scim-me.html>`_.  If it cannot be determined,
    or to use a different user for the path, the user name may be specified using this option.
    """,
)
def repo(
    user_name: str,
    source: str,
    full_sync: bool,
    dry_run: bool,
    include_dirs: List[str],
    force_include_dirs: List[str],
    dest_repo: str,
    exclude_dirs: List[str],
    profile: str,
    watch: bool,
    polling_interval_secs: float,
    include_patterns: List[str],
    force_include_patterns: List[str],
    exclude_patterns: List[str],
    use_gitignore: bool,
    delete_unmatched_option: DeleteUnmatchedOption,
):
    """
    Syncs from source directory to a `Databricks Repo <https://docs.databricks.com/repos/index.html>`_.
    """

    # watch defaults to true, so to make it easy to just add --dry-run without having to add --no-watch,
    # we'll set watch to false here.
    if dry_run:
        watch = False

    config = get_databricks_config(profile)

    if not user_name:
        user_name = get_user_name(config)

    if not user_name:
        raise click.UsageError(
            "Destination repo path can't be automatically determined because the user is not known. "
            "Please either specify the user with --user."
        )

    source = handle_source(source)

    matcher = create_path_matcher(
        source=source,
        include_dirs=include_dirs,
        exclude_dirs=exclude_dirs,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
        use_gitignore=use_gitignore,
        force_include_dirs=force_include_dirs,
        force_include_patterns=force_include_patterns,
    )

    client = ReposClient(user=user_name, repo_name=dest_repo, config=config)

    main_loop(
        source=source,
        matcher=matcher,
        client=client,
        full_sync=full_sync,
        dry_run=dry_run,
        watch=watch,
        polling_interval_secs=polling_interval_secs,
        delete_unmatched_option=delete_unmatched_option,
    )
