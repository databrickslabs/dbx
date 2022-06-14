import subprocess
import sys
import shlex

from dbx.utils import dbx_echo


class BuildManager:
    @staticmethod
    def build_core_package(flavour: str = "wheel"):
        if flavour == "wheel":
            BuildManager._build_wheel()

    @staticmethod
    def _build_wheel():
        dbx_echo("Re-building package")
        subprocess.check_call([sys.executable] + shlex.split("pip wheel -w dist -e . --prefer-binary"))
        dbx_echo("Package re-build finished")
