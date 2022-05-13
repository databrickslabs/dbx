import os
import tempfile
from tests.unit.sync.utils import mocked_props

import pytest

from tests.unit.sync.utils import temporary_directory


@pytest.fixture
def mock_config():
    return mocked_props(token="fake-token", host="http://fakehost.asdf/base/", insecure=None)


@pytest.fixture
def dummy_file_path() -> str:
    with temporary_directory() as tempdir:
        file_path = os.path.join(tempdir, "file")
        with open(file_path, "w") as f:
            f.write("yo")
        yield file_path
