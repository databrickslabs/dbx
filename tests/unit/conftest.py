import contextlib
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Callable
from unittest.mock import MagicMock
from uuid import uuid4

import mlflow
import pytest
from databricks_cli.configure.provider import DatabricksConfig
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from dbx.api.client_provider import DatabricksClientProvider
from dbx.api.launch.pipeline_models import PipelineGlobalState
from dbx.api.storage.io import StorageIO
from dbx.api.storage.mlflow_based import MlflowStorageConfigurationManager
from dbx.cli import app
from dbx.commands.init import init
from dbx.utils.file_uploader import MlflowFileUploader
from tests.unit.api.launch.test_pipeline_runner import TEST_PIPELINE_ID, TEST_PIPELINE_UPDATE_PAYLOAD

TEST_HOST = "https:/dbx.cloud.databricks.com"
TEST_TOKEN = "dapiDBXTEST"
DEFAULT_DEPLOYMENT_FILE_PATH = Path("conf/deployment.json")

test_dbx_config = DatabricksConfig.from_token(TEST_HOST, TEST_TOKEN)


def extract_function_name(func: Callable) -> str:
    return f"{func.__module__}.{func.__name__}"  # noqa


def get_path_with_relation_to_current_file(p: str):
    return Path(__file__).parent.joinpath(str(p)).resolve()


def invoke_cli_runner(*args, **kwargs):
    """
    Helper method to invoke the CliRunner while asserting that the exit code is actually 0.
    """
    expected_error = kwargs.pop("expected_error") if "expected_error" in kwargs else None
    res = CliRunner().invoke(app, *args, **kwargs)

    if res.exit_code != 0:
        if not expected_error:
            logging.error("Exception in the cli runner: %s" % res.exception)
            raise res.exception
        else:
            logging.info("Expected exception in the cli runner: %s" % res.exception)

    return res


def initialize_cookiecutter(project_name):
    init(template="python_basic", path=None, package=None, parameters=[f"project_name={project_name}"], no_input=True)


@contextlib.contextmanager
def in_context(path):
    """Changes working directory and returns to previous on exit."""
    prev_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


@pytest.fixture(scope="function", autouse=False)
def temp_project(tmp_path: Path, mocker: MockerFixture, request) -> Path:
    project_name = "dev_dbx_%s" % str(uuid4()).split("-")[0]
    logging.info("Launching test in directory %s with project name %s" % (tmp_path, project_name))

    with in_context(tmp_path):
        initialize_cookiecutter(project_name)

    project_path = tmp_path / project_name

    def generate_wheel(*args, **kwargs):
        wheel_file = Path(".").absolute() / "dist" / f"{project_name}-0.0.1-py3-none-any.whl"
        wheel_file.parent.mkdir(exist_ok=True)
        wheel_file.write_bytes(b"a")

    with in_context(project_path):
        if "disable_auto_execute_mock" in request.keywords:
            logging.info("Disabling the execute_shell_command for specific test")
        else:
            mocker.patch("dbx.models.build.execute_shell_command", generate_wheel)
        yield project_path


@pytest.fixture(scope="session", autouse=True)
def mlflow_fixture(session_mocker):
    """
    This fixture provides local instance of mlflow with support for tracking and registry functions.
    After the test session:
    * temporary storage for tracking and registry is deleted.
    * Active run will be automatically stopped to avoid verbose errors.
    :return: None
    """

    logging.info("Configuring local Mlflow instance")
    tracking_uri = tempfile.TemporaryDirectory().name
    registry_uri = f"sqlite:///{tempfile.TemporaryDirectory().name}"

    mlflow.set_tracking_uri(Path(tracking_uri).as_uri())
    mlflow.set_registry_uri(registry_uri)
    session_mocker.patch.object(MlflowStorageConfigurationManager, "prepare", MagicMock())
    # we introduce this mock since all files uploaded to the local tracking URI
    # will start with file:/// by default, so FileUploader will try to load them once again.

    logging.info("Mlflow instance configured")
    yield None

    mlflow.end_run()

    if Path(tracking_uri).exists():
        shutil.rmtree(tracking_uri)

    if Path(registry_uri).exists():
        Path(registry_uri).unlink()
    logging.info("Test session finished, unrolling the Mlflow instance")


@pytest.fixture(scope="function")
def mlflow_file_uploader(mocker, mlflow_fixture):
    mocker.patch.object(MlflowFileUploader, "_verify_fuse_support", MagicMock())
    mocker.patch.object(MlflowFileUploader, "_upload_file", MagicMock())

    def _mocked_processor(local_file_path: Path, as_fuse) -> str:
        remote_path = "/".join(["dbfs:/mocks/testing", str(local_file_path.as_posix())])
        remote_path = remote_path.replace("dbfs:/", "/dbfs/") if as_fuse else remote_path
        return remote_path

    mocker.patch.object(MlflowFileUploader, "_postprocess_path", MagicMock(side_effect=_mocked_processor))


@pytest.fixture()
def mock_storage_io(mocker):
    mocker.patch.object(StorageIO, "save", MagicMock())


@pytest.fixture()
def mock_api_v2_client(mocker):
    mocker.patch.object(DatabricksClientProvider, "get_v2_client", MagicMock())


@pytest.fixture()
def mock_api_v1_client(mocker):
    mocker.patch.object(DatabricksClientProvider, "get_v2_client", MagicMock())


@pytest.fixture
def pipeline_launch_mock(mocker: MockerFixture):
    client = MagicMock()
    client.perform_query = MagicMock(
        side_effect=[
            {"statuses": [{"pipeline_id": TEST_PIPELINE_ID, "name": "some"}]},  # get pipeline
            {"state": PipelineGlobalState.RUNNING},  # get current state
            {},  # stop pipeline
            {"state": PipelineGlobalState.IDLE},  # second verification get
            TEST_PIPELINE_UPDATE_PAYLOAD,  # start pipeline
        ]
    )
    return client
