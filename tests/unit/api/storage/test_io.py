import mlflow

from dbx.api.storage.io import StorageIO


def test_storage_serde():
    payload = {"a": 1}
    with mlflow.start_run() as _run:
        StorageIO.save(payload, "content.json")
        result = StorageIO.load(_run.info.run_id, "content.json")
        assert result == payload
