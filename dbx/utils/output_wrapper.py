import os
from typing import Optional

from dbx.utils import dbx_echo


class OutputWrapper:
    def __init__(self, symbol: Optional[str] = "="):
        self._symbol = symbol
        self._terminal_width = os.get_terminal_size().columns

    def __enter__(self):
        dbx_echo(self._symbol * self._terminal_width)

    def __exit__(self, exc_type, exc_val, exc_tb):
        dbx_echo(self._symbol * self._terminal_width)
