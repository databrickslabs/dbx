import os
import re

import click
from cookiecutter.main import cookiecutter
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS
from path import Path

import dbx
from dbx.cli.clusters import DEV_CLUSTER_FILE
from dbx.cli.utils import LockFileController, write_json, dbx_echo

TEMPLATE_PATH = os.path.join(dbx.__path__[0], "template")


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Initializes a plain new project in a new directory.')
@click.option("--project-name", required=True, type=str, help="Name for a new project")
@click.option('--cloud', required=True, type=click.Choice(['Azure', 'AWS'], case_sensitive=True), help="Cloud provider")
@click.option('--pipeline-engine', required=True,
              type=click.Choice(['GitHub Actions', 'Azure Pipelines'], case_sensitive=True),
              help="Pipeline engine type")
@click.option("--override", is_flag=True, required=False, default=False, type=bool,
              help="Override existing project settings if directory exists")
@debug_option
def init(**kwargs):
    """
    Initializes a plain new project in a new directory
    """
    verify_project_name(kwargs["project_name"])
    cookiecutter(TEMPLATE_PATH, extra_context=kwargs, no_input=True, overwrite_if_exists=kwargs["override"])

    dbx_echo("Initializing project in directory: %s" % os.path.join(os.getcwd(), kwargs["project_name"]))

    with Path(kwargs["project_name"]):
        lockfile = LockFileController()  # initializes the lockfile with unique uuid per each project instance
        write_json(kwargs, ".dbx.json")
        dev_cluster_name = "dev-%s-%s" % (kwargs["project_name"], lockfile.get_uuid())
        dbx_echo("Lockfile prepared")

        if kwargs["cloud"] == "AWS":
            dev_cluster_spec = get_aws_cluster_spec(dev_cluster_name)
        elif kwargs["cloud"] == "Azure":
            dev_cluster_spec = get_azure_cluster_spec(dev_cluster_name)
        else:
            raise ValueError("This cloud provider is not yet supported in dbx: %s" % kwargs["cloud"])

        write_json(dev_cluster_spec, DEV_CLUSTER_FILE)
        dbx_echo("Project initialization finished")


def verify_project_name(project_name):
    name_regex = r'^[_a-zA-Z][_a-zA-Z0-9]+$'
    if not re.match(name_regex, project_name):
        raise ValueError("Project name %s is not a valid python package name" % project_name)


def get_azure_cluster_spec(cluster_name):
    spec = {
        "autoscale": {
            "min_workers": 2,
            "max_workers": 8
        },
        "cluster_name": cluster_name,
        "spark_version": "7.0.x-cpu-ml-scala2.12",
        "spark_conf": {"spark.databricks.conda.condaMagic.enabled": True},
        "node_type_id": "Standard_F4s",
        "ssh_public_keys": [],
        "custom_tags": {},
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "autotermination_minutes": 60,
        "init_scripts": []
    }
    return spec


def get_aws_cluster_spec(cluster_name):
    spec = {
        "autoscale": {
            "min_workers": 2,
            "max_workers": 8
        },
        "cluster_name": cluster_name,
        "spark_version": "7.0.x-cpu-ml-scala2.12",
        "spark_conf": {"spark.databricks.conda.condaMagic.enabled": True},
        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-west-2c",
            "instance_profile_arn": None,
            "spot_bid_price_percent": 100,
            "ebs_volume_type": "GENERAL_PURPOSE_SSD",
            "ebs_volume_count": 3,
            "ebs_volume_size": 100
        },
        "node_type_id": "c4.2xlarge",
        "ssh_public_keys": [],
        "custom_tags": {},
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "autotermination_minutes": 60,
        "init_scripts": []
    }
    return spec
