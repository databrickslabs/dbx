import pytest

from dbx.models.workflow.v2dot1.workflow import GitSource


def test_git_source_positive():
    gs = GitSource(git_url="http://some", git_provider="some", git_branch="some")
    assert gs.git_branch == "some"


def test_git_source_negative():
    with pytest.raises(ValueError):
        GitSource(git_url="http://some", git_provider="some", git_branch="some", git_tag="some")
