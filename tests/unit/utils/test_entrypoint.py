import shlex
import subprocess
import sys


def test_main_version():
    result = subprocess.check_call([sys.executable] + shlex.split("-m dbx --version"))
    assert result == 0


def test_main_help():
    result = subprocess.check_call([sys.executable] + shlex.split("-m dbx --help"))
    assert result == 0
