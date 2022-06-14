import os
import pathlib
from typing import List, Any

from pydantic import BaseModel

from dbx.models.base import FlexibleBaseModel
from dbx.models.clusters import NewCluster
from dbx.models.deployment import WorkloadDefinition
from dbx.models.libraries import Library
from dbx.models.tasks import TaskDefinition
from dbx.utils import dbx_echo
from dbx.utils.file_uploader import MlflowFileUploader
from dbx.utils.named_properties import (
    ExistingClusterNamePreprocessor,
    NewClusterPropertiesProcessor,
    PolicyNameProcessor,
)


class AdjustmentManager:
    # TODO: Add core library recognition and packaging options
    def __init__(self, workloads: List[WorkloadDefinition], uploader: MlflowFileUploader):
        self._existing_cluster_name_preprocessor = ExistingClusterNamePreprocessor()
        self._new_cluster_processor = NewClusterPropertiesProcessor()
        self._policy_name_processor = PolicyNameProcessor()
        self._uploader = uploader
        self._workloads = workloads

    @staticmethod
    def _verify_file_exists(local_path: pathlib.Path):
        if not local_path.exists():
            raise FileNotFoundError(
                f"Path {local_path} is referenced in the deployment configuration, but is non-existent."
            )

    def _path_adjustment(self, candidate: str) -> str:
        if candidate.startswith("file:"):
            fuse_flag = candidate.startswith("file:fuse:")
            replace_string = "file:fuse://" if fuse_flag else "file://"
            local_path = pathlib.Path(candidate.replace(replace_string, ""))

            self._verify_file_exists(local_path)

            adjusted_path = self._uploader.upload_and_provide_path(local_path, as_fuse=fuse_flag)
            return adjusted_path

        else:
            return candidate

    def _adjust_path(self, candidate):
        if isinstance(candidate, str):
            # path already adjusted or points to another dbfs object - pass it
            if candidate.startswith("dbfs") or candidate.startswith("/dbfs"):
                return candidate
            else:
                adjusted_path = self._path_adjustment(candidate)
                return adjusted_path
        else:
            return candidate

    @staticmethod
    def _update_candidate_value(parent, index, new):
        if isinstance(parent, dict) | isinstance(parent, list):
            parent[index] = new
        elif issubclass(type(parent), (FlexibleBaseModel, BaseModel)):
            parent.__setattr__(index, new)

    @staticmethod
    def _find_package_file():
        dbx_echo("Locating package file")
        file_locator = list(pathlib.Path("dist").glob("*.whl"))
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

    def _add_core_package(self, task: TaskDefinition):
        core_package_file = self._find_package_file()
        if core_package_file:
            library_def = Library(whl=f"file://{core_package_file}")
            task.libraries.append(library_def)
        else:
            dbx_echo(
                "Core package file was not found."
                "Please check your /dist/ folder if you expect to use package-based imports"
            )

    def _adjust(self, parent: Any, index: Any, candidate: Any):
        """
        Main function which goes recursively through all elements of the workload and performs adjustments.
        Adjustments are based on property and type matching.
        :return:
        """
        if candidate:
            if isinstance(candidate, str):
                _adjusted = self._adjust_path(candidate)
                if _adjusted != candidate:
                    self._update_candidate_value(parent, index, _adjusted)

            elif isinstance(candidate, dict):
                for key, sub_element in candidate.items():
                    self._adjust(candidate, key, sub_element)
            elif isinstance(candidate, list):
                for idx, sub_element in enumerate(candidate):
                    self._adjust(candidate, idx, sub_element)
            elif issubclass(type(candidate), (FlexibleBaseModel, BaseModel)):
                if isinstance(candidate, NewCluster):
                    self._new_cluster_processor.process(candidate)
                    new = self._policy_name_processor.process(candidate)
                    self._update_candidate_value(parent, index, new)
                elif isinstance(candidate, TaskDefinition):
                    self._existing_cluster_name_preprocessor.process(candidate)
                    if not candidate.custom_properties.disable_core_package_reference:
                        self._add_core_package(candidate)
                for name, sub_element in candidate:
                    self._adjust(candidate, name, sub_element)

    def adjust_workloads(self) -> List[WorkloadDefinition]:
        for workload in self._workloads:
            self._adjust(parent=None, index=None, candidate=workload)
        return self._workloads
