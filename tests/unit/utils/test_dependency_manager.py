from pathlib import Path
import textwrap
from dbx.utils.dependency_manager import DependencyManager


def write_requirements(parent: Path, content: str) -> Path:
    _file = parent / "requirements.txt"
    _file.write_text(textwrap.dedent(content))
    return _file


def test_simple_requirements_file(tmp_path: Path):
    requirements_txt = write_requirements(
        tmp_path,
        """\
        tqdm
        rstcheck
        prospector>=1.3.1,<1.7.0""",
    )

    dm = DependencyManager(
        no_rebuild=True,
        global_no_package=True,
        requirements_file=requirements_txt.resolve(),
    )
    assert dm._requirements_references == [
        {"pypi": {"package": "tqdm"}},
        {"pypi": {"package": "rstcheck"}},
        {"pypi": {"package": "prospector<1.7.0,>=1.3.1"}},
    ]


def test_requirements_with_comments(tmp_path: Path):
    requirements_txt = write_requirements(
        tmp_path,
        """\
        # simple comment
        tqdm
        rstcheck # use this library
        prospector>=1.3.1,<1.7.0""",
    )

    dm = DependencyManager(
        no_rebuild=True,
        global_no_package=True,
        requirements_file=requirements_txt.resolve(),
    )
    assert dm._requirements_references == [
        {"pypi": {"package": "tqdm"}},
        {"pypi": {"package": "rstcheck"}},
        {"pypi": {"package": "prospector<1.7.0,>=1.3.1"}},
    ]


def test_requirements_with_empty_line(tmp_path):
    requirements_txt = write_requirements(
        tmp_path,
        """\
        tqdm
        rstcheck
        prospector>=1.3.1,<1.7.0""",
    )

    dm = DependencyManager(
        no_rebuild=True,
        global_no_package=True,
        requirements_file=requirements_txt.resolve(),
    )
    assert dm._requirements_references == [
        {"pypi": {"package": "tqdm"}},
        {"pypi": {"package": "rstcheck"}},
        {"pypi": {"package": "prospector<1.7.0,>=1.3.1"}},
    ]


def test_requirements_with_filtered_pyspark(tmp_path):
    requirements_txt = write_requirements(
        tmp_path,
        """\
        tqdm
        pyspark==1.2.3
        rstcheck
        prospector>=1.3.1,<1.7.0""",
    )

    dm = DependencyManager(
        no_rebuild=True,
        global_no_package=True,
        requirements_file=requirements_txt.resolve(),
    )
    assert dm._requirements_references == [
        {"pypi": {"package": "tqdm"}},
        {"pypi": {"package": "rstcheck"}},
        {"pypi": {"package": "prospector<1.7.0,>=1.3.1"}},
    ]


def test_not_matching_conditions(tmp_path, capsys):

    dm = DependencyManager(no_rebuild=True, global_no_package=True, requirements_file=None)

    reference = {"deployment_config": {"no_package": False}}

    dm.process_dependencies(reference)
    captured = capsys.readouterr()
    assert "--no-package option is set to true" in captured.out
