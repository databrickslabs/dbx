import shutil
import tempfile
from pathlib import Path
from typing import List

from fastapi import FastAPI, UploadFile

app = FastAPI()
APP_PORT = 6006
APP_HOST = "0.0.0.0"
SERVER_ROOT_PATH = Path(tempfile.mkdtemp(prefix="dbx"))


@app.on_event("shutdown")
def shutdown_event():
    shutil.rmtree(SERVER_ROOT_PATH)


@app.get("/info")
async def root():
    return {"status": "working", "root_path": SERVER_ROOT_PATH}


@app.post("/upload/{context_id}")
async def upload_files(context_id: str, files: List[UploadFile]):
    in_context_path = SERVER_ROOT_PATH / context_id

    if not in_context_path.exists():
        in_context_path.mkdir()

    for _file in files:
        content = _file.file.read()
        _file_path = in_context_path / _file.filename
        _file_path.write_bytes(content)



