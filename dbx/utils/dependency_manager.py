from typing import Optional, Dict, Union, Any

from dbx.utils import dbx_echo
from dbx.utils.common import handle_package, get_package_file

# this type alias represents a library reference, for example:
# {"whl": "path/to/some/file"}
# {"pypi": "some-pypi-package"}
# {"pypi": {"package": "some-package"}}

LibraryReference = Dict[str, Union[str, Dict[str, Any]]]


class DependencyManager:
    """
    This class manages dependency references in the job or task deployment.
    """

    def __init__(self, global_no_package: bool, no_rebuild: bool):
        self._global_no_package = global_no_package
        self._no_rebuild = no_rebuild
        self._core_package_reference: Optional[LibraryReference] = self._get_package_requirement()

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
                return {"whl": f"file://{package_file}"}
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

        if not no_package_reference:
            reference["libraries"] += [self._core_package_reference]
