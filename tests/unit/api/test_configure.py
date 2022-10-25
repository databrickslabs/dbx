import shutil
from pathlib import Path

import pytest

from dbx.api.configure import ProjectConfigurationManager


def test_configure_non_existent_project(temp_project: Path):
    shutil.rmtree(temp_project / ".dbx")
    with pytest.raises(FileNotFoundError):
        ProjectConfigurationManager().get("default")


def test_configure_jinja_support(temp_project: Path):
    _manager = ProjectConfigurationManager()
    _manager.enable_jinja_support()
    assert _manager.get_jinja_support()


def test_configure_enable_custom_init_scripts(temp_project: Path):
    _manager = ProjectConfigurationManager()
    _manager.enable_custom_init_scripts()
    assert _manager.get_custom_init_scripts()
