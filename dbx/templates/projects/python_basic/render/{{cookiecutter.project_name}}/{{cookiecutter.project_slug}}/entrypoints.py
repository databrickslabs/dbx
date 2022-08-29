import sys

import pytest


def pytest_main():  # pragma: no cover
    pytest.main(sys.argv[1:])
