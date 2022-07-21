from pathlib import Path
from typing import Optional


def verify_jinja_variables_file(_, __, value: Optional[Path]):
    if value:
        if value.suffix not in [".yaml", ".yml"]:
            raise Exception("Jinja variables file shall be provided in yaml or yml format")
        if not value.exists():
            raise FileNotFoundError(f"Jinja variables file option is not empty, but file is non-existent {value}")
