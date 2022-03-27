import json
import pathlib
from typing import Optional

from dbx.constants import LOCK_FILE_PATH


class LocalContextManager:
    def __init__(self, context_file_path: Optional[str] = LOCK_FILE_PATH):
        self._file = pathlib.Path(context_file_path)

    def set_context(self, context_id: str) -> None:
        self._file.write_text(json.dumps({"context_id": context_id}), encoding="utf-8")

    def get_context(self) -> Optional[str]:
        if self._file.exists():
            return json.loads(self._file.read_text(encoding="utf-8")).get("context_id")
        else:
            return None
