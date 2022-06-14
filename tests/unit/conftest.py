import contextlib
import logging
import os
import pathlib
import random
import shutil
import tempfile
from pathlib import Path
from typing import List, Dict
from unittest.mock import MagicMock
from uuid import uuid4

from click.testing import CliRunner

from dbx.api.clients.databricks_api import DatabricksClientProvider
from dbx.api.jobs import AdvancedJobsService
from dbx.api.mlflow import MlflowStorageConfigurationManager
import mlflow
import pytest

from dbx.commands.init import init
from dbx.models.deployment import WorkloadDefinition
from dbx.utils.file_uploader import MlflowFileUploader


def invoke_cli_runner(*args, **kwargs):
    """
    Helper method to invoke the CliRunner while asserting that the exit code is actually 0.
    """
    expected_error = kwargs.pop("expected_error") if kwargs.get("expected_error") else None

    res = CliRunner().invoke(*args, **kwargs)

    if res.exit_code != 0:
        if not expected_error:
            logging.error("Exception in the cli runner: %s" % res.exception)
            raise res.exception
        else:
            logging.info("Expected exception in the cli runner: %s" % res.exception)

    return res


def initialize_cookiecutter(project_name):
    invoke_cli_runner(
        init,
        ["-p", f"project_name={project_name}", "--no-input"],
    )


@contextlib.contextmanager
def in_context(path):
    """Changes working directory and returns to previous on exit."""
    prev_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


@pytest.fixture(scope="function", autouse=True)
def temp_project(tmp_path: pathlib.Path):
    project_name = "dev_dbx_%s" % str(uuid4()).split("-")[0]
    logging.info("Launching test in directory %s with project name %s" % (tmp_path, project_name))

    with in_context(tmp_path):
        initialize_cookiecutter(project_name)

    project_path = tmp_path / project_name

    with in_context(project_path):
        yield project_path


@pytest.fixture(scope="session", autouse=True)
def v2_client_fixture(session_mocker):
    session_mocker.patch.object(DatabricksClientProvider, "get_v2_client", MagicMock())


@pytest.fixture(scope="function")
def job_creation_fixture(mocker):
    def mocked_mapping(workloads: List[WorkloadDefinition]) -> Dict[str, int]:
        return {w.name: random.randint(0, 1000) for w in workloads}

    mocker.patch.object(
        AdvancedJobsService, "create_jobs_and_get_id_mapping", MagicMock(side_effect=mocked_mapping)
    )


@pytest.fixture(scope="session", autouse=True)
def mlflow_fixture(session_mocker):
    """
    This fixture provides local instance of mlflow with support for tracking and registry functions.
    After the test session:
    * temporary storage for tracking and registry is deleted.
    * Active run will be automatically stopped to avoid verbose errors.
    :return: None
    """
    session_mocker.patch.object(MlflowStorageConfigurationManager, "prepare", MagicMock())

    session_mocker.patch.object(
        MlflowFileUploader, "_convert_to_fuse_path", MagicMock(side_effect=(lambda p: p.replace("file:///", "")))
    )

    logging.info("Configuring local Mlflow instance")
    tracking_uri = tempfile.TemporaryDirectory().name
    registry_uri = f"sqlite:///{tempfile.TemporaryDirectory().name}"

    mlflow.set_tracking_uri(Path(tracking_uri).as_uri())
    mlflow.set_registry_uri(registry_uri)
    logging.info("Mlflow instance configured")
    yield None

    mlflow.end_run()

    if Path(tracking_uri).exists():
        shutil.rmtree(tracking_uri)

    if Path(registry_uri).exists():
        Path(registry_uri).unlink()
    logging.info("Test session finished, unrolling the Mlflow instance")
