from __future__ import annotations

from pathlib import Path
from typing import List

import pkg_resources

from dbx.models.workflow.common.libraries import Library, PythonPyPiLibrary
from dbx.utils import dbx_echo


class RequirementsFileProcessor:
    def __init__(self, requirements_file: Path):
        self._requirements_file = requirements_file
        self._libraries = self.parse_requirements()

    @property
    def libraries(self) -> List[Library]:
        return self._libraries

    @staticmethod
    def _delete_managed_libraries(packages: List[pkg_resources.Requirement]) -> List[pkg_resources.Requirement]:
        output_packages = []
        for package in packages:
            if package.key == "pyspark":
                dbx_echo("pyspark dependency deleted from the list of libraries, because it's a managed library")
            else:
                output_packages.append(package)
        return output_packages

    def parse_requirements(self) -> List[Library]:
        with self._requirements_file.open(encoding="utf-8") as requirements_txt:
            requirements_content = pkg_resources.parse_requirements(requirements_txt)
            filtered_libraries = self._delete_managed_libraries(requirements_content)
            libraries = [Library(pypi=PythonPyPiLibrary(package=str(req))) for req in filtered_libraries]
            return libraries
