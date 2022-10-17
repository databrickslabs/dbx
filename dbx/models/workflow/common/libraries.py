from typing import Optional, List

from pydantic import root_validator

from dbx.models.validators import at_least_one_of, mutually_exclusive
from dbx.models.workflow.common.flexible import FlexibleModel


class PythonPyPiLibrary(FlexibleModel):
    package: str
    repo: Optional[str]


class MavenLibrary(FlexibleModel):
    coordinates: str
    repo: Optional[str]
    exclusions: Optional[List[str]]


class RCranLibrary(FlexibleModel):
    package: str
    repo: Optional[str]


class Library(FlexibleModel):
    jar: Optional[str]
    egg: Optional[str]
    whl: Optional[str]
    pypi: Optional[PythonPyPiLibrary]
    maven: Optional[MavenLibrary]
    cran: Optional[RCranLibrary]

    @root_validator(pre=True)
    def _validate(cls, values):  # noqa
        at_least_one_of(cls.get_field_names(), values)
        mutually_exclusive(cls.get_field_names(), values)
        return values
