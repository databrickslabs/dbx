from enum import Enum


class ApiVersion(str, Enum):
    v2dot0 = "v2dot0"
    v2dot1 = "v2dot1"

    undefined = "undefined"


class ApiVersionMixin:
    def get_api_version(self) -> ApiVersion:  # noqa
        module_name = self.__class__.__module__

        if ApiVersion.v2dot0 in module_name:
            return ApiVersion.v2dot0
        elif ApiVersion.v2dot1 in module_name:
            return ApiVersion.v2dot1
        else:
            return ApiVersion.undefined
