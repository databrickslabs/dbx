import sys

import pytest

if __name__ == "__main__":
    exit_code = pytest.main(sys.argv[1:])
    if exit_code != pytest.ExitCode.OK:
        raise RuntimeError(f"pytest returned non-zero exit code: {str(exit_code)}. See logs for details.")
