import shutil
from pathlib import Path

import pytest

from dbx.api.configure import ConfigurationManager


def test_configure_non_existent_project(temp_project: Path):
    shutil.rmtree(temp_project / ".dbx")
    with pytest.raises(FileNotFoundError):
        ConfigurationManager().get("default")
