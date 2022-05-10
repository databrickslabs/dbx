import os
from contextlib import contextmanager
from tempfile import TemporaryDirectory

from unittest.mock import AsyncMock, MagicMock, PropertyMock


def mocked_props(**props):
    obj = MagicMock()
    for k, v in props.items():
        setattr(type(obj), k, PropertyMock(return_value=v))
    return obj


def create_async_with_result(result):
    return_value = AsyncMock()
    return_value.__aenter__.return_value = result
    return_value.__aexit__.return_value = None
    return return_value


@contextmanager
def temporary_directory():
    with TemporaryDirectory() as tempdir:
        yield os.path.realpath(tempdir)


@contextmanager
def pushd(d):
    previous = os.getcwd()
    os.chdir(d)
    try:
        yield
    finally:
        os.chdir(previous)
