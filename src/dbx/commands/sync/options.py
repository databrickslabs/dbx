import typer

from dbx.sync import DeleteUnmatchedOption

SOURCE_OPTION = typer.Option(
    None,
    "--source",
    "-s",
    exists=True,
    dir_okay=True,
    file_okay=False,
    help="""The local source path to sync from.
        If the current working directory is a git repo, then the tool by default uses that path as the source.

        Otherwise the source path will need to be specified.""",
)

FULL_SYNC_OPTION = typer.Option(
    False,
    "--full-sync",
    is_flag=True,
    help="""Ignores any existing sync state and syncs all files and directories matching the filters to the
        destination.""",
)
DRY_RUN_OPTION = typer.Option(
    False, "--dry-run", is_flag=True, help="Log what the tool would do without making any changes."
)

INCLUDE_DIRS_OPTION = typer.Option(
    None,
    "--include",
    "-i",
    help="""A directory to sync, relative to the source directory.  This directory must exist.
        When this option is used, no files or directories will be synced unless specifically included
        by this or other include options.

        For example:

        * `-i foo` will sync a directory `foo` directly under the source directory

        * `-i foo/bar` will sync a directory `foo/bar` directly under the source directory""",
)

FORCE_INCLUDE_DIRS_OPTION = typer.Option(
    None,
    "--force-include",
    "-fi",
    help="""A directory to sync, relative to the source directory. This directory must exist.


        When this option is used, no files or directories will be synced unless specifically included
        by this or other include options.


        Unlike `--include`, this will sync a directory regardless of files/directories that are excluded from
        syncing.  This can be useful when, for example, the `.gitignore` lists a directory that you want to have
        synced.  The patterns in the `.gitignore` are used by default to exclude files/directories from syncing.


        For example:

        * `-i foo` will sync a directory `foo` directly under the source directory

        * `-i foo/bar` will sync a directory `foo/bar` directly under the source directory""",
)

EXCLUDE_DIRS_OPTION = typer.Option(
    None,
    "--exclude",
    "-e",
    help="""A directory to exclude from syncing, relative to the source directory. This directory must exist.

        For example:

        * `-e foo` will exclude directory `foo` directly under the source directory from syncing

        * `-e foo/bar` will exclude directory `foo/bar` directly under the source directory from syncing
        """,
)

INCLUDE_PATTERNS_OPTION = typer.Option(
    None,
    "--include-pattern",
    "-ip",
    help="""A pattern specifying files and/or directories to sync, relative to the source directory.
        This option uses the same format as .gitignore.


        When this option is used, no files or directories will be synced unless specifically included
        by this or other include options.


        For example:


        * `foo` will match any file or directory named `foo` anywhere under the source

        * `/foo/` will only match a directory named `foo` directly under the source.

        * `*.py` will match all Python files.

        * `/foo/*.py` will match all Python files directly under the `foo` directory.

        * `/foo/**/*.py` will match all Python files anywhere under the `foo` directory.

        You may also store a list of patterns inside a`.syncinclude` file under the source path.
        Patterns in this file will be used as the default patterns to include. This essentially behaves
        as the opposite of a .gitignore file, but with the same format.
        """,
)

FORCE_INCLUDE_PATTERNS_OPTION = typer.Option(
    None,
    "--force-include-pattern",
    "-fip",
    help="""A pattern specifying files and/or directories to sync, relative to the source directory, regardless
        of whether these files and/or directories would otherwise be excluded.

        See documentation of `--include-pattern` for usage.""",
)

UNMATCHED_BEHAVIOUR_OPTION = typer.Option(
    DeleteUnmatchedOption.UNSPECIFIED_DELETE_UNMATCHED,
    "--unmatched-behaviour",
    help="""Specifies how to handle files/directories that would be deleted in the remote destination because
        they don't match the current set of filters.


        For example, suppose you have used the option`-i foo` to sync only the `foo` directory and later
        quit the tool.  Then suppose you restart the tool using`-i bar` to sync only the `bar` directory.
        In this situation, it is unclear whether your intention is to 1) sync over `bar` and remove `foo` in the
        destination, or 2) sync over `bar` and leave `foo` alone in the destination.  Due to this ambiguity, the tool
        will ask to confirm your intentions.


        To avoid having to confirm, you can use either of these options:

        * `--unmatched-behaviour=allow-delete-unmatched` will delete files/directories in the destination
          that are not present locally with the current filters.
          So for the example above, this would remove `foo` in the destination when syncing with`-i bar`.

        * `--unmatched-behaviour=allow-delete-unmatched=disallow-delete-unmatched` will NOT delete files/directories
          in the destination that are not present locally with the current filters.
          So for the example above, this would leave `foo` in the destination when syncing with`-i bar`.""",
)

EXCLUDE_PATTERNS_OPTION = typer.Option(
    None,
    "--exclude-pattern",
    "-ep",
    help="""A pattern specifying files and/or directories to exclude from syncing, relative to the source directory.


        This uses the same format as `.gitignore`.
        For examples, see the documentation of`--include-pattern`.""",
)

WATCH_OPTION = typer.Option(
    True,
    "--watch/--no-watch",
    is_flag=True,
    help="""Controls whether the tool should watch for file changes after the initial sync.


        With`--watch`, which is the default, it will watch for file system changes
        and rerun the sync whenever any changes occur to files or directories matching the filters.


        With`--no-watch` the tool will quit after the initial sync.""",
)

POLLING_INTERVAL_OPTION = typer.Option(
    None,
    "--polling-interval",
    help="Use file system polling instead of file system events and set the polling interval (in seconds)",
)

USE_GITIGNORE_OPTION = typer.Option(
    True,
    "--use-gitignore/--no-use-gitignore",
    is_flag=True,
    help="""Controls whether the .gitignore is used to automatically exclude file/directories from syncing.""",
)

SYNC_ENVIRONMENT_OPTION = typer.Option(None, "--environment", help="Environment name.")
