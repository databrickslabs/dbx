import inspect
from pathlib import Path
from typing import Optional

import typer
from rich.prompt import Prompt
from typer.rich_utils import _get_rich_console  # noqa

from dbx.api.config_reader import ConfigReader
from dbx.api.destroyer import Destroyer
from dbx.models.destroyer import DestroyerConfig, DeletionMode
from dbx.options import (
    WORKFLOW_ARGUMENT,
    DEPLOYMENT_FILE_OPTION,
    ENVIRONMENT_OPTION,
    JINJA_VARIABLES_FILE_OPTION,
)
from dbx.utils import dbx_echo
from dbx.utils.common import prepare_environment


def destroy(
    workflow: Optional[str] = WORKFLOW_ARGUMENT,
    workflows: Optional[str] = typer.Option(
        None, "--workflows", help="Comma-separated list of workflow names to be deleted", show_default=False
    ),
    deployment_file: Optional[Path] = DEPLOYMENT_FILE_OPTION,
    environment: str = ENVIRONMENT_OPTION,
    jinja_variables_file: Optional[Path] = JINJA_VARIABLES_FILE_OPTION,
    deletion_mode: DeletionMode = typer.Option(
        DeletionMode.all,
        "--mode",
        help="""Deletion mode.


        If `assets-only`, will only delete the stored assets in the artifact storage, but won't affect job objects.


        If `workflows-only`, will only delete the defined job objects, but won't affect job objects.


        If `all`, will delete everything.""",
    ),
    confirm: bool = typer.Option(
        False,
        "--confirm",
        help="Disable the confirmation dialog and accept the consequences of this action",
        is_flag=True,
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Don't delete objects, just show what would be deleted", is_flag=True
    ),
    dracarys: bool = typer.Option(
        False, "--dracarys", help="🔥 add more fire to the CLI output, making the deletion absolutely **epic**."
    ),
):
    if workflow and workflows:
        raise Exception(f"arguments {workflow} and {workflows} cannot be provided together")

    _workflows = [workflow] if workflow else workflows.split(",") if workflows else []

    config_reader = ConfigReader(deployment_file, jinja_variables_file)
    config = config_reader.get_config()

    _d_config = DestroyerConfig(
        workflows=_workflows,
        deletion_mode=deletion_mode,
        dracarys=dracarys,
        deployment=config.get_environment(environment, raise_if_not_found=True),
        dry_run=dry_run,
    )

    if dry_run:
        dbx_echo(
            "Omitting the confirmation check since it's a dry run. "
            "For a real run the confirmation check will be requested"
        )
    else:
        if not confirm:
            ask_for_confirmation(_d_config)

    api_client = prepare_environment(environment)
    destroyer = Destroyer(api_client, _d_config)

    destroyer.launch()


def ask_for_confirmation(conf: DestroyerConfig):
    header = """\
        🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨
        The following action is going to [bold]irreversibly[/bold] delete selected
        workflows and (or) assets in your Databricks environment.
        🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨
    """

    if conf.deletion_mode == DeletionMode.assets_only:
        deletion_message = "All assets will de deleted, but the workflow definitions won't be affected."
    elif conf.deletion_mode == DeletionMode.workflows_only:
        deletion_message = (
            f"The following workflows are marked for deletion: {conf.workflows}, assets won't be affected"
        )
    else:
        deletion_message = f"""The following workflows are marked for deletion: {conf.workflows}.
            [bold]All assets are also marked for deletion.[/bold]"""

    _c = _get_rich_console()
    _c.print(inspect.cleandoc(header))
    _c.print(inspect.cleandoc(deletion_message))
    _c.print("=" * _c.width)

    responsibility = """\
        [code]dbx[/code] and it's maintainers are [bold]not responsible[/bold] for any kind of
        technical and financial implications that are following the deletion.

        [red bold]If you understand the consequences of this action and
        take the responsibility for the potential consequences, type 'yes'[/red bold]
        """

    result = Prompt.ask(
        inspect.cleandoc(responsibility),
        choices=["yes", "no"],
    )
    if result != "yes":
        dbx_echo("Deletion has been cancelled.")
        raise typer.Exit()
