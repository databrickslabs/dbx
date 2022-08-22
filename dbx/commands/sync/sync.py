from typing import List, Optional

import click
import typer
from databricks_cli.configure.provider import ProfileConfigProvider

from dbx.api.configure import ProjectConfigurationManager
from dbx.commands.sync.functions import (
    create_path_matcher,
    main_loop,
    handle_source,
    get_user_name,
    get_source_base_name,
)
from dbx.commands.sync.options import (
    SOURCE_OPTION,
    FULL_SYNC_OPTION,
    DRY_RUN_OPTION,
    INCLUDE_DIRS_OPTION,
    FORCE_INCLUDE_DIRS_OPTION,
    EXCLUDE_DIRS_OPTION,
    WATCH_OPTION,
    POLLING_INTERVAL_OPTION,
    INCLUDE_PATTERNS_OPTION,
    FORCE_INCLUDE_PATTERNS_OPTION,
    EXCLUDE_PATTERNS_OPTION,
    USE_GITIGNORE_OPTION,
    UNMATCHED_BEHAVIOUR_OPTION,
    SYNC_ENVIRONMENT_OPTION,
)
from dbx.options import PROFILE_OPTION
from dbx.sync import DeleteUnmatchedOption
from dbx.sync.clients import DBFSClient, ReposClient
from dbx.sync.config import get_databricks_config
from dbx.utils import dbx_echo

sync_app = typer.Typer(
    short_help="🔄 Sync local files to Databricks and watch for changes.",
    help="""🔄 Sync local files to Databricks and watch for changes.

    Sync local files to Databricks and watch for changes, with support for syncing to either a path
    in [DBFS](https://docs.databricks.com/data/databricks-file-system.html) or a
    [Databricks Repo](https://docs.databricks.com/repos/index.html) via the `dbfs` and `repo`
    subcommands.


    This enables one to incrementally sync local files to Databricks in order to enable quick, iterative
    development in an IDE with the ability to test changes almost immediately in Databricks notebooks.


    Suppose you are using the [Repos for Git integration](https://docs.databricks.com/repos/index.html) feature
    and have cloned a git repo within Databricks where you have Python notebooks stored as well as various Python
    modules that the notebooks import.


    You can edit any of these files directly in Databricks.
    The `dbx sync repo` command provides an additional option: edit the files in a local repo on your computer
    in an IDE of your choice and sync the changes to the repo in Databricks as you make changes.


    For example, when run from a local git clone, the following will sync all the files to an existing repo
    named `myrepo` in Databricks and watch for changes:

    ```
    dbx sync repo -d myrepo
    ```

    At the top of your notebook you can turn on
    [autoreload](https://ipython.org/ipython-doc/3/config/extensions/autoreload.html) so that execution of cells
    will automatically pick up the changes:

    ```
    %load_ext autoreload
    %autoreload 2
    ```

    The `dbx sync repo` command syncs to a repo in Databricks. If that repo is a git clone you can see the
    changes made to the files, as if you'd made the edits directly in Databricks.


    Alternatively, you can use `dbx sync dbfs` to sync the files to a path in DBFS.
    This keeps the files independent from the repos but still allows you to use them in notebooks
    either in a repo or in notebooks existing in your workspace.


    For example, when run from a local git clone in a `myrepo` directory under a user
    `first.last@somewhere.com`, the following will sync all the files to the DBFS path
    `/tmp/users/first.last/myrepo`:

    ```
    dbx sync dbfs
    ```

    The destination path can also be specified, as in: `-d /tmp/myrepo`.

    When executing notebooks in a repo, the root of the repo is automatically added to the Python path so that
    imports work relative to the repo root. This means that aside from turning on autoreload you don't need to do
    anything else special for the changes to be reflected in the cell's execution.

    However, when syncing to DBFS, for the imports to work you need to update the Python path to include
    this target directory you're syncing to.


    For example, to import from the `/tmp/users/first.last/myrepo` path used above, use the following at the top
    of your notebook:

    ```python
    import sys

    if "/dbfs/tmp/users/first.last/myrepo" not in sys.path:
        sys.path.insert(0, "/dbfs/tmp/users/first.last/myrepo")
    ```

    The `dbx sync` commands have many options for controlling which files/directories to include/exclude from
    syncing, which are well documented below.  For convenience, all patterns listed in a `.gitignore` at the
    source will be excluded from syncing. The `.git` directory is excluded as well.""",
)


@sync_app.command(
    short_help="📁 Syncs from a source directory to DBFS",
)
def dbfs(
    user_name: Optional[str] = typer.Option(
        None,
        "--user",
        "-u",
        help="""Specify the user name to use when constructing the default destination path.

        This has no effect when `--dest` is already specified.  If this is an email address then the domain is
        ignored.

        For example `-u first.last` and  `-u first.last@somewhere.com` will both result
        in  `first.last` as the user name.""",
    ),
    source: Optional[str] = SOURCE_OPTION,
    full_sync: bool = FULL_SYNC_OPTION,
    dry_run: bool = DRY_RUN_OPTION,
    include_dirs: Optional[List[str]] = INCLUDE_DIRS_OPTION,
    force_include_dirs: Optional[List[str]] = FORCE_INCLUDE_DIRS_OPTION,
    dest_path: Optional[str] = typer.Option(
        None,
        "--dest",
        "-d",
        help="""A path in DBFS to sync to.
            For example,  `-d /tmp/project` would sync from the local source path to the DBFS path  `/tmp/project`.

            Specifying this path is optional. By default the tool will sync to the destination
            `/tmp/users/<user_name>/<source_base_name>`.

            For example, given local source path  `/foo/bar` and Databricks user  `first.last@somewhere.com`,
            this would sync to  `/tmp/users/first.last/bar`.


            This path is chosen as a safe default option that is unlikely to overwrite anything important.


            When constructing this default destination path, the user name is determined using the scim/me API.
            If it cannot be determined or to use a different user for the path, you may use the  `--user` option.""",
    ),
    exclude_dirs: Optional[List[str]] = EXCLUDE_DIRS_OPTION,
    profile: str = PROFILE_OPTION,
    environment: Optional[str] = SYNC_ENVIRONMENT_OPTION,
    watch: bool = WATCH_OPTION,
    polling_interval_secs: Optional[float] = POLLING_INTERVAL_OPTION,
    include_patterns: Optional[List[str]] = INCLUDE_PATTERNS_OPTION,
    force_include_patterns: Optional[List[str]] = FORCE_INCLUDE_PATTERNS_OPTION,
    exclude_patterns: Optional[List[str]] = EXCLUDE_PATTERNS_OPTION,
    use_gitignore: bool = USE_GITIGNORE_OPTION,
    delete_unmatched_option: DeleteUnmatchedOption = UNMATCHED_BEHAVIOUR_OPTION,
):
    # watch defaults to true, so to make it easy to just add --dry-run without having to add --no-watch,
    # we'll set watch to false here.
    if dry_run:
        watch = False

    if environment:
        dbx_echo("Environment option is provided, therefore environment-based config will be used")
        _info = ProjectConfigurationManager().get(environment)
        config = ProfileConfigProvider(_info.profile).get_config()
    else:
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


@sync_app.command(
    short_help="""
    🔀 Syncs from a source directory to a Databricks Repo
    """,
    help="""
    🔀 Syncs from a source directory to a Databricks Repo
    """,
)
def repo(
    user_name: Optional[str] = typer.Option(
        None,
        "--user",
        "-u",
        help="""The user who owns the Databricks Repo to sync to.

            Repos exist in the Databricks workspace under a path  of the form  `/Repos/<user>/<repo>`.
            This specifies the `<user>` portion of the path.

            This is optional, as the user name is determined automatically using the scim/me API.

            If it cannot be determined, or to use a different user for the path,
            the user name may be specified using this option.""",
    ),
    source: Optional[str] = SOURCE_OPTION,
    full_sync: bool = FULL_SYNC_OPTION,
    dry_run: bool = DRY_RUN_OPTION,
    include_dirs: Optional[List[str]] = INCLUDE_DIRS_OPTION,
    force_include_dirs: Optional[List[str]] = FORCE_INCLUDE_DIRS_OPTION,
    dest_repo: str = typer.Option(
        ...,
        "--dest-repo",
        "-d",
        help="""The name of the `Databricks Repo <https://docs.databricks.com/repos/index.html>`_ to sync to.

            Repos exist in the Databricks workspace under a path  of the form  `/Repos/<user>/<repo>`.
            This specifies the `<repo>` portion of the path.""",
    ),
    exclude_dirs: Optional[List[str]] = EXCLUDE_DIRS_OPTION,
    profile: str = PROFILE_OPTION,
    environment: str = SYNC_ENVIRONMENT_OPTION,
    watch: bool = WATCH_OPTION,
    polling_interval_secs: Optional[float] = POLLING_INTERVAL_OPTION,
    include_patterns: Optional[List[str]] = INCLUDE_PATTERNS_OPTION,
    force_include_patterns: Optional[List[str]] = FORCE_INCLUDE_PATTERNS_OPTION,
    exclude_patterns: Optional[List[str]] = EXCLUDE_PATTERNS_OPTION,
    use_gitignore: bool = USE_GITIGNORE_OPTION,
    delete_unmatched_option: DeleteUnmatchedOption = UNMATCHED_BEHAVIOUR_OPTION,
):
    # watch defaults to true, so to make it easy to just add --dry-run without having to add --no-watch,
    # we'll set watch to false here.
    if dry_run:
        watch = False

    if environment:
        dbx_echo("Environment option is provided, therefore environment-based config will be used")
        _info = ProjectConfigurationManager().get(environment)
        config = ProfileConfigProvider(_info.profile).get_config()
    else:
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
