from types import TracebackType
from typing import Optional, Type

import mlflow
from mlflow.entities import Run


class EmptyContext:
    def __enter__(self):
        yield

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ):
        if exc_val:
            raise exc_val


class AssetBasedLaunchContext:
    def __init__(self, deployment_run: Run):
        self._deployment_run = deployment_run

    def __enter__(self):
        with mlflow.start_run(run_id=self._deployment_run.info.run_id):
            with mlflow.start_run(nested=True):
                yield

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ):
        mlflow.end_run()
        if exc_val:
            raise exc_val
