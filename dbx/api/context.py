import json
import pathlib
from typing import Optional

from dbx.constants import LOCK_FILE_PATH


class LocalContextManager:
    context_file_path: pathlib.Path = LOCK_FILE_PATH

    @classmethod
    def set_context(cls, context_id: str) -> None:
        cls.context_file_path.write_text(json.dumps({"context_id": context_id}), encoding="utf-8")

    @classmethod
    def get_context(cls) -> Optional[str]:
        if cls.context_file_path.exists():
            return json.loads(cls.context_file_path.read_text(encoding="utf-8")).get("context_id")
        else:
            return None
