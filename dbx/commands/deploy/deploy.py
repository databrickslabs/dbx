import pathlib
from typing import List
from typing import Optional

import click
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.api.deploy import DeploymentManager, DeploymentKind
from dbx.commands.deploy.preprocessors import DeploymentArgumentsPreprocessor
from dbx.utils import dbx_echo
from dbx.utils.cli import parse_list_of_arguments
from dbx.utils.common import get_current_branch_name
from dbx.utils.dependency_manager import BuildManager
from dbx.utils.options import environment_option
from dbx.commands.deploy.options import common_deploy_options


@click.command(
    context_settings=CONTEXT_SETTINGS,
    short_help="""Deploy workloads to the Databricks and create job objects.""",
    help="""Deploy workloads to the Databricks and create job objects.""",
)
@common_deploy_options
@debug_option
@environment_option
def deploy_job(**kwargs):
    deploy_common(kind=DeploymentKind.JOB, **kwargs)


@click.command(
    context_settings=CONTEXT_SETTINGS,
    short_help="""Deploy workloads to the Databricks in snapshot mode, without creating or changing the jobs""",
    help="""Deploy workloads to the Databricks in snapshot mode, without creating or changing the jobs""",
)
@common_deploy_options
@debug_option
@environment_option
def deploy_snapshot(**kwargs):
    deploy_common(kind=DeploymentKind.SNAPSHOT, **kwargs)


def deploy_common(
    environment: str,
    workload_name: Optional[str],
    all: Optional[bool],  # noqa
    deployment_file: Optional[pathlib.Path],
    tags: List[str],
    no_rebuild: bool,
    save_final_definitions: Optional[pathlib.Path],
    branch_name: Optional[str],
    kind: DeploymentKind,
):
    dbx_echo(f"Starting new deployment for environment {environment}")
    tags = parse_list_of_arguments(tags)
    tags["branch_name"] = branch_name if branch_name else get_current_branch_name()
    deployment_file = DeploymentArgumentsPreprocessor.preprocess_deployment_file(deployment_file)
    _env = DeploymentArgumentsPreprocessor.preprocess_config(deployment_file, environment)

    workloads = DeploymentArgumentsPreprocessor.preprocess_workload(_env, workload_name, all)

    if not no_rebuild:
        BuildManager.build_core_package()

    manager = DeploymentManager(
        workloads=workloads,
        env_name=environment,
        additional_tags=tags,
        save_final_definitions=save_final_definitions,
        kind=kind,
    )
    manager.deploy()
