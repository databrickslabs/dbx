import json
import shutil
import tempfile
from pathlib import Path
from typing import Dict, Any

import mlflow
from mlflow.tracking import MlflowClient

from dbx.utils.json import JsonUtils


class StorageIO:
    @staticmethod
    def save(content: Dict[Any, Any], name: str):
        temp_dir = tempfile.mkdtemp()
        serialized_data = json.dumps(content, indent=4)
        temp_path = Path(temp_dir, name)
        temp_path.write_text(serialized_data, encoding="utf-8")
        mlflow.log_artifact(str(temp_path), ".dbx")
        shutil.rmtree(temp_dir)

    @staticmethod
    def load(run_id: str, file_name: str) -> Dict[Any, Any]:
        client = MlflowClient()
        with tempfile.TemporaryDirectory() as tmp:
            dbx_file_path = f".dbx/{file_name}"
            client.download_artifacts(run_id, dbx_file_path, tmp)
            return JsonUtils.read(Path(tmp) / dbx_file_path)
