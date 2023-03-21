import os

import pytest

from tests.unit.sync.utils import mocked_props
from tests.unit.sync.utils import temporary_directory


@pytest.fixture
def mock_config():
    return mocked_props(token="fake-token", host="http://fakehost.asdf/?o=1234", insecure=None)


@pytest.fixture
def dummy_file_path() -> str:
    with temporary_directory() as tempdir:
        file_path = os.path.join(tempdir, "file")
        with open(file_path, "w") as f:
            f.write("yo")
        yield file_path


@pytest.fixture
def dummy_file_path_2mb() -> str:
    with temporary_directory() as tempdir:
        file_path = os.path.join(tempdir, "file")
        with open(file_path, "w") as f:
            f.write("y" * 1024 * 2048)
        yield file_path
