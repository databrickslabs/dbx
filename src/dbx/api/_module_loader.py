import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def load_module_from_source(source: Path, module_name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, str(source.absolute()))
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module
