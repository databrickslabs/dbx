import os
from typing import Optional

from dbx.utils import dbx_echo


class OutputWrapper:
    def __init__(self, symbol: Optional[str] = "="):
        self._cols = os.get_terminal_size().columns
        self._symbol = symbol

    def __enter__(self):
        dbx_echo(self._symbol * self._cols)

    def __exit__(self, exc_type, exc_val, exc_tb):
        dbx_echo(self._symbol * self._cols)
