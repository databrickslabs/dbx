import shlex
import subprocess
import sys


def test_main():
    result = subprocess.check_call([sys.executable] + shlex.split("-m dbx --version"))
    assert result == 0
