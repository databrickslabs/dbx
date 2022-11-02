from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dbx.models.build import BuildConfiguration
from dbx.models.workflow.common.libraries import Library
from dbx.utils import dbx_echo


class CorePackageManager:
    def __init__(self, build_config: BuildConfiguration):
        self.build_config = build_config
        self._core_package: Optional[Library] = self.prepare_core_package()

    @property
    def core_package(self) -> Optional[Library]:
        return self._core_package

    def prepare_core_package(self) -> Optional[Library]:
        self.build_config.trigger_build_process()
        package_file = self.get_package_file()

        if package_file:
            return Library(whl=f"file://{package_file}")
        else:
            dbx_echo(
                "Package file was not found. Please check the dist folder if you expect to use package-based imports"
            )

    @staticmethod
    def get_package_file() -> Optional[Path]:
        dbx_echo("Locating package file")
        file_locator = list(Path("dist").glob("*.whl"))
        sorted_locator = sorted(
            file_locator, key=os.path.getmtime
        )  # get latest modified file, aka latest package version
        if sorted_locator:
            file_path = sorted_locator[-1]
            dbx_echo(f"Package file located in: {file_path}")
            return file_path
        else:
            dbx_echo("Package file was not found")
            return None
