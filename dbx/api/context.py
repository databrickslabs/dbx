from pathlib import Path
from typing import Optional

from dbx.utils.json import JsonUtils


class LocalContextManager:
    context_file_path: Path = Path.home() / ".dbx" / "lock.json"

    @classmethod
    def set_context(cls, context_id: str) -> None:
        JsonUtils.write(cls.context_file_path, {"context_id": context_id})

    @classmethod
    def get_context(cls) -> Optional[str]:
        if cls.context_file_path.exists():
            return JsonUtils.read(cls.context_file_path).get("context_id")
        else:
            return None
