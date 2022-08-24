import inspect
from pathlib import Path
from typing import Optional

import typer
from rich.prompt import Prompt

from dbx.api.config_reader import ConfigReader
from dbx.api.destroyer import Destroyer, DestroyerConfig
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
    workflows_only: bool = typer.Option(
        False,
        "--workflows-only",
        help="Only delete the job objects without affecting the assets and artifact storage",
        is_flag=True,
    ),
    assets_only: bool = typer.Option(
        False,
        "--assets-only",
        help="Only delete the assets defined in the artifact storage without affecting the job objects",
        is_flag=True,
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

    if dry_run:
        dbx_echo("Omitting the confirmation check since it's a dry run")
    else:
        if not confirm:
            ask_for_confirmation()

    _workflows = [workflow] if workflow else workflows.split(",") if workflows else []

    api_client = prepare_environment(environment)

    config_reader = ConfigReader(deployment_file, jinja_variables_file)
    config = config_reader.get_config()

    _d_config = DestroyerConfig(
        workflows=_workflows,
        workflows_only=workflows_only,
        assets_only=assets_only,
        dracarys=dracarys,
        deployment=config.get_environment(environment, raise_if_not_found=True),
        dry_run=dry_run,
    )

    destroyer = Destroyer(api_client, _d_config)

    destroyer.launch()


def ask_for_confirmation():
    _text = """\
        🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨
        The following action is going to [bold]irreversibly[/bold] delete selected
        workflows and (or) assets in your Databricks environment.
        🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨

        [code]dbx[/code] and it's maintainers are [bold]not responsible[/bold] for any kind of
        technical and financial implications that are following the deletion.

        [red bold]If you understand the consequences of this action and
        take the responsibility for it, type 'yes'[/red bold]
        """

    result = Prompt.ask(
        inspect.cleandoc(_text),
        choices=["yes", "no"],
    )
    if result != "yes":
        dbx_echo("Deletion has been cancelled.")
        raise typer.Exit()
