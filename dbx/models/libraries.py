from typing import Optional, List

from pydantic import BaseModel


class PypiLibrary(BaseModel):
    package: str
    repo: Optional[str]


class CranLibrary(BaseModel):
    package: str
    repo: Optional[str]


class MavenLibrary(BaseModel):
    coordinates: str
    repo: Optional[str]
    exclusions: Optional[List[str]]


class Library(BaseModel):
    jar: Optional[str]
    egg: Optional[str]
    whl: Optional[str]
    pypi: Optional[PypiLibrary]
    cran: Optional[CranLibrary]
    maven: Optional[MavenLibrary]
