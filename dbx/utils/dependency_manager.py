import pathlib
from typing import Optional, Dict, List, Union, Any

from dbx.utils.common import dbx_echo, handle_package, get_package_file

# this type alias represents a library reference, for example:
# {"whl": "path/to/some/file"}
# {"pypi": "some-pypi-package"}
# {"pypi": {"package": "some-package"}}

LibraryReference = Dict[str, Union[str, Dict[str, Any]]]


class DependencyManager:
    """
    This class manages dependency references in the job or task deployment.
    """

    def __init__(
        self, global_no_package: bool, no_rebuild: bool, strict_adjustment: bool, requirements_file: Optional[str]
    ):
        self._global_no_package = global_no_package
        self._no_rebuild = no_rebuild
        self._strict_adjustment = strict_adjustment
        self._core_package_reference: Optional[LibraryReference] = self._get_package_requirement()
        self._requirements_references: List[LibraryReference] = self._get_requirements_from_file(requirements_file)

    @staticmethod
    def _delete_managed_libraries(packages: List[str]) -> List[str]:
        output_packages = []

        for package in packages:

            if package == "pyspark" or package.startswith("pyspark="):
                dbx_echo("pyspark dependency deleted from the list of libraries, because it's a managed library")
            else:
                output_packages.append(package)

        return output_packages

    def _get_requirements_from_file(self, requirements_file: Optional[str]) -> List[LibraryReference]:
        if not requirements_file:
            dbx_echo("No requirements file was provided")
            return []
        else:
            requirements_path = pathlib.Path(requirements_file)

            if not requirements_path.exists():
                dbx_echo("Requirements file was not found")
                return []
            else:
                requirements_content = requirements_path.read_text(encoding="utf-8").split("\n")
                filtered_libraries = self._delete_managed_libraries(requirements_content)

                requirements_payload = [{"pypi": {"package": req}} for req in filtered_libraries if req]
                return requirements_payload

    def _get_package_requirement(self) -> Optional[LibraryReference]:
        """
        Prepare package requirement to be added into the definition in case it's required.
        """
        handle_package(self._no_rebuild)
        package_file = get_package_file()

        if self._global_no_package:
            dbx_echo("No package definition will be added into any jobs in the given deployment")
            return None
        else:
            if package_file:
                file_reference = str(package_file) if not self._strict_adjustment else f"file://{package_file}"
                return {"whl": file_reference}
            else:
                dbx_echo(
                    "Package file was not found! "
                    "Please check your /dist/ folder if you expect to use package-based imports"
                )
                return None

    def process_dependencies(self, reference: Dict[str, Any]):
        reference_level_deployment_config = reference.get("deployment_config", {})
        no_package_reference = reference_level_deployment_config.get("no_package", False)

        if self._global_no_package and not no_package_reference:
            dbx_echo(
                ":warning: Global --no-package option is set to true, "
                "but task or job level deployment config is set to false. "
                "Global-level property will take priority."
            )

        reference["libraries"] = reference.get("libraries", []) + self._requirements_references

        if not no_package_reference:
            reference["libraries"] += [self._core_package_reference]
