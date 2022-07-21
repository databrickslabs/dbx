import json
from pathlib import Path
from typing import Dict, Any

JsonContent = Dict[Any, Any]


class JsonUtils:
    @staticmethod
    def read(file_path: Path) -> JsonContent:
        return json.loads(file_path.read_text(encoding="utf-8"))

    @staticmethod
    def write(file_path: Path, content: JsonContent):
        file_path.write_text(json.dumps(content, indent=4), encoding="utf-8")
