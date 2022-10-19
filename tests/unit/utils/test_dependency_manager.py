import textwrap
from pathlib import Path

import pytest

from dbx.api.dependency.requirements import RequirementsFileProcessor
from dbx.models.workflow.common.libraries import Library, PythonPyPiLibrary


def write_requirements(parent: Path, content: str) -> Path:
    _file = parent / "requirements.txt"
    _file.write_text(textwrap.dedent(content))
    return _file


@pytest.mark.parametrize(
    "req_payload",
    [
        """\
        tqdm
        rstcheck
        prospector>=1.3.1,<1.7.0""",
        """\
        # simple comment
        tqdm
        rstcheck # use this library
        prospector>=1.3.1,<1.7.0""",
        """\
        tqdm
        rstcheck
        prospector>=1.3.1,<1.7.0""",
    ],
)
def test_simple_requirements_file(req_payload, tmp_path: Path):
    requirements_txt = write_requirements(tmp_path, req_payload)

    parsed = RequirementsFileProcessor(requirements_txt).parse_requirements()
    assert parsed == [
        Library(pypi=PythonPyPiLibrary(package="tqdm")),
        Library(pypi=PythonPyPiLibrary(package="rstcheck")),
        Library(pypi=PythonPyPiLibrary(package="prospector<1.7.0,>=1.3.1")),
    ]
