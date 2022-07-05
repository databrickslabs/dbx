import shutil
import tempfile
from pathlib import Path
from typing import List

from fastapi import FastAPI, UploadFile

from dbx.driver_server.models import TempStorage, ServerInfo

app = FastAPI()

APP_PORT = 6006
APP_HOST = "0.0.0.0"

SERVER_DRIVER_TMP_PATH = Path(tempfile.mkdtemp(prefix="dbx"))

server_info = ServerInfo(
    temp_storage=TempStorage(driver_path=SERVER_DRIVER_TMP_PATH)
)


@app.on_event("shutdown")
def shutdown_event():
    shutil.rmtree(server_info.temp_storage.driver_path)


@app.get("/info")
async def info():
    return server_info


@app.post("/upload/{context_id}")
async def upload_driver_files(context_id: str, files: List[UploadFile]):
    in_context_path = SERVER_DRIVER_TMP_PATH / context_id

    if not in_context_path.exists():
        in_context_path.mkdir()

    for _file in files:
        content = _file.file.read()
        _file_path = in_context_path / _file.filename
        _file_path.write_bytes(content)
