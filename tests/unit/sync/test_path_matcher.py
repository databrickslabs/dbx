import os
from pathlib import Path

import pytest

from dbx.commands.sync import create_path_matcher
from dbx.sync.path_matcher import PathMatcher

from .utils import temporary_directory


def test_no_rules():
    """
    Tests that with no rules, it matches everything.
    """
    with temporary_directory() as tempdir:
        tempdir = Path(tempdir)

        matcher = PathMatcher(tempdir)

        (tempdir / "foo").mkdir()
        (tempdir / "foo" / "bar").touch()
        (tempdir / "baz").touch()

        # never match the source itself
        assert not matcher.match(tempdir)

        assert matcher.match(tempdir / "foo")
        assert matcher.match(f"{tempdir}/foo")
        assert matcher.match(tempdir / "foo" / "bar")
        assert matcher.match(tempdir / "baz")

        # only match files within the directory, not the directory itself
        assert not matcher.match(tempdir)
        assert not matcher.match(f"{tempdir}/")


def test_invalid_root():
    """
    Tests that calling should_ignore on a path inconsistent with the root path is ignored.
    """
    with temporary_directory() as tempdir:
        with temporary_directory() as tempdir2:
            tempdir = Path(tempdir)
            tempdir2 = Path(tempdir2)
            matcher = PathMatcher(tempdir)

            (tempdir / "foo").touch()
            (tempdir2 / "foo").touch()

            # sometimes events happen outside the target directory, which we ignore
            assert not matcher.match(tempdir2 / "foo")


def test_include_spec():
    """
    Tests that should_ignore can properly follow included patterns.
    """
    with temporary_directory() as tempdir:
        tempdir = Path(tempdir)

        matcher = PathMatcher(tempdir, includes=["foo/"])

        (tempdir / "foo").mkdir()
        (tempdir / "foo" / "bar").touch()
        (tempdir / "bar").mkdir()
        (tempdir / "baz").mkdir()
        (tempdir / "bar" / "foo").touch()
        (tempdir / "baz" / "foo").mkdir()
        (tempdir / "baz" / "foo" / "wee").touch()
        (tempdir / "bar" / "bop").touch()

        # never match the source itself
        assert not matcher.match(tempdir)
        assert not matcher.match(f"{tempdir}/")

        # everything under the foo directory is included
        assert matcher.match(tempdir / "foo")
        assert matcher.match(tempdir / "foo" / "bar")

        # a foo file in a subdirectory is not included
        assert not matcher.match(tempdir / "bar" / "foo")

        # but a foo directory in a subdirectory is, as well as anything under it
        assert matcher.match(tempdir / "baz" / "foo")
        assert matcher.match(tempdir / "baz" / "foo" / "wee")

        assert not matcher.match(tempdir / "bar" / "bop")

        assert not matcher.should_ignore(tempdir / "foo")


def test_ignore_spec():
    """
    Tests that should_ignore can properly follow ignored patterns.
    """
    with temporary_directory() as tempdir:
        tempdir = Path(tempdir)

        matcher = PathMatcher(tempdir, ignores=["foo/"])

        (tempdir / "foo").mkdir()
        (tempdir / "foo" / "bar").touch()
        (tempdir / "bar").mkdir()
        (tempdir / "baz").mkdir()
        (tempdir / "bar" / "foo").touch()
        (tempdir / "baz" / "foo").mkdir()
        (tempdir / "baz" / "foo" / "wee").touch()
        (tempdir / "bar" / "bop").touch()

        # everything under the foo directory is included
        assert not matcher.match(tempdir / "foo")
        assert not matcher.match(tempdir / "foo" / "bar")

        # a foo file in a subdirectory is not included
        assert matcher.match(tempdir / "bar" / "foo")

        # but a foo directory in a subdirectory is, as well as anything under it
        assert not matcher.match(tempdir / "baz" / "foo")
        assert not matcher.match(tempdir / "baz" / "foo" / "wee")

        assert matcher.match(tempdir / "bar" / "bop")

        assert matcher.should_ignore(tempdir / "foo")


def test_ignore_and_include_spec():
    """
    Tests that should_ignore can properly follow included and ignored patterns when used together.
    """
    with temporary_directory() as tempdir:
        tempdir = Path(tempdir)

        matcher = PathMatcher(tempdir, includes=["foo/"], ignores=["foo/bar/"])

        (tempdir / "foo").mkdir()
        (tempdir / "foo" / "bar").mkdir()
        (tempdir / "foo" / "baz").mkdir()
        (tempdir / "foo" / "bar" / "bat").touch()

        # everything under the foo directory is included except for bar/
        assert matcher.match(tempdir / "foo")
        assert not matcher.match(tempdir / "foo" / "bar")
        assert not matcher.match(tempdir / "foo" / "bar" / "bat")
        assert matcher.match(tempdir / "foo" / "baz")


def test_create_path_matcher():
    """
    Tests using create_path_matcher to create the PathMatcher instead.
    """
    with temporary_directory() as tempdir:
        tempdir = Path(tempdir)

        (tempdir / "foo").mkdir()
        (tempdir / "foo" / "bar").touch()

        (tempdir / "baz").mkdir()
        (tempdir / "baz" / "foo").touch()

        matcher = create_path_matcher(source=tempdir, includes=None, excludes=None)

        assert matcher.match(tempdir / "foo")
        assert matcher.match(tempdir / "foo" / "bar")

        assert matcher.match(tempdir / "baz")
        assert matcher.match(tempdir / "baz" / "foo")


def test_create_path_matcher_with_gitignore():
    """
    Tests using create_path_matcher to automatically handle .gitignore and .git.
    """
    with temporary_directory() as tempdir:
        tempdir = Path(tempdir)

        (tempdir / "foo").mkdir()
        (tempdir / "foo" / "bar").touch()

        # should be ignored
        (tempdir / "baz").mkdir()
        (tempdir / "baz" / "foo").touch()

        # ignored by default
        (tempdir / ".git").mkdir()
        (tempdir / ".git" / "foo").touch()

        # ignored by default
        (tempdir / "blah.isorted").touch()

        with open(os.path.join(tempdir, ".gitignore"), "w") as gitignore:
            gitignore.write("bop/\n")

        # should be ignored
        (tempdir / "bop").mkdir()
        (tempdir / "bop" / "foo").touch()

        matcher = create_path_matcher(source=tempdir, excludes=["baz"], includes=None)

        assert matcher.match(tempdir / "foo")
        assert matcher.match(tempdir / "foo" / "bar")

        assert not matcher.match(tempdir / "baz")
        assert not matcher.match(tempdir / "baz" / "foo")

        assert not matcher.match(tempdir / ".git")
        assert not matcher.match(tempdir / ".git" / "foo")

        assert not matcher.match(tempdir / "blah.isorted")

        assert not matcher.match(tempdir / "bop")
        assert not matcher.match(tempdir / "bop" / "foo")


def test_create_path_matcher_with_syncinclude():
    """
    Tests using create_path_matcher to automatically handle .syncinclude.
    """
    with temporary_directory() as tempdir:
        tempdir = Path(tempdir)

        # should be ignored
        (tempdir / "foo").mkdir()
        (tempdir / "foo" / "bar").touch()

        # should be ignored
        (tempdir / "baz").mkdir()
        (tempdir / "baz" / "foo").touch()

        with open(os.path.join(tempdir, ".syncinclude"), "w") as syncinclude:
            syncinclude.write("/bop/\n")

        # should be included due to .syncinclude
        (tempdir / "bop").mkdir()
        (tempdir / "bop" / "foo").touch()

        matcher = create_path_matcher(source=tempdir, excludes=None, includes=None)

        assert not matcher.match(tempdir / "foo")
        assert not matcher.match(tempdir / "foo" / "bar")

        assert not matcher.match(tempdir / "baz")
        assert not matcher.match(tempdir / "baz" / "foo")

        assert matcher.match(tempdir / "bop")
        assert matcher.match(tempdir / "bop" / "foo")


def test_create_path_matcher_with_syncinclude_and_includes():
    """
    Tests using create_path_matcher to ignore .syncinclude when there are explicit includes.
    """
    with temporary_directory() as tempdir:
        tempdir = Path(tempdir)

        # included due to explicit include
        (tempdir / "foo").mkdir()
        (tempdir / "foo" / "bar").touch()

        # should be ignored
        (tempdir / "baz").mkdir()
        (tempdir / "baz" / "foo").touch()

        with open(os.path.join(tempdir, ".syncinclude"), "w") as syncinclude:
            syncinclude.write("/bop/\n")

        # despite being in .syncinclude, the includes overrides it
        (tempdir / "bop").mkdir()
        (tempdir / "bop" / "foo").touch()

        matcher = create_path_matcher(source=tempdir, excludes=None, includes=["/foo/"])

        assert matcher.match(tempdir / "foo")
        assert matcher.match(tempdir / "foo" / "bar")

        assert not matcher.match(tempdir / "baz")
        assert not matcher.match(tempdir / "baz" / "foo")

        assert not matcher.match(tempdir / "bop")
        assert not matcher.match(tempdir / "bop" / "foo")


def test_nonexistent_file():
    """
    Tests that the matcher can be applied to files that don't exist.
    """
    with temporary_directory() as tempdir:
        tempdir = Path(tempdir)
        matcher = PathMatcher(tempdir, includes=["foo/", "bar"])

        # "foo/" will only match directories
        assert not matcher.match(tempdir / "foo")
        assert matcher.match(tempdir / "foo", is_directory=True)
        assert not matcher.match(tempdir / "foo", is_directory=False)
        assert not matcher.match(f"{tempdir}/foo")
        assert matcher.match(f"{tempdir}/foo", is_directory=True)
        assert not matcher.match(f"{tempdir}/foo", is_directory=False)
        assert matcher.match(f"{tempdir}/foo/")

        # "bar" matches either files or directories, so this will match all cases
        assert matcher.match(tempdir / "bar")
        assert matcher.match(tempdir / "bar", is_directory=True)
        assert matcher.match(tempdir / "bar", is_directory=False)
        assert matcher.match(f"{tempdir}/bar")
        assert matcher.match(f"{tempdir}/bar", is_directory=True)
        assert matcher.match(f"{tempdir}/bar", is_directory=False)
        assert matcher.match(f"{tempdir}/bar/")

        with pytest.raises(ValueError):
            matcher.match(f"{tempdir}/bar/", is_directory=False)
