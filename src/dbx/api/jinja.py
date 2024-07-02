import os
from pathlib import Path
from typing import Optional

from dbx.utils import dbx_echo


def get_last_modified_file(path: str, extension: str) -> Optional[str]:
    """

    Provides last modified file from the given path with the provided extension.

    :param path:
    :param extension:
    :return:
    """
    file_locator = Path(path).glob(f"*.{extension}")
    sorted_locator = sorted(file_locator, key=os.path.getmtime)
    if sorted_locator:
        file_path = sorted_locator[-1]
        dbx_echo(f"Found latest file in provided path {path}")
        return str(file_path)
    else:
        dbx_echo(f"No file was found in path {path}")
        return None
