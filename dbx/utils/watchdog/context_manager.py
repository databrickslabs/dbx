import asyncio

from dbx.utils.common import ApiV1Client, ContextLockFile
from dbx.utils.watchdog.cluster_manager import ClusterManager


class ContextManager:
    def __init__(self, api_v1_client: ApiV1Client, cluster_manager: ClusterManager):
        self._api_v1_client = api_v1_client
        self._cluster_manager = cluster_manager
        self._status = "initializing"
        self.context_id = ContextLockFile.get_context()

    @property
    def status(self):
        return self._status

    async def _create_context(self, language: str = "python"):
        payload = {'language': language, 'clusterId': self._cluster_manager.cluster_id}
        response = self._api_v1_client.create_context(payload)
        return response["id"]

    async def _get_context_status_info(self):
        payload = {"clusterId": self._cluster_manager.cluster_id, "contextId": self.context_id}
        status_info = self._api_v1_client.get_context_status(payload)
        return status_info

    async def _creation_routine(self):
        self._status = "creating a new context"
        self.context_id = await self._create_context()
        ContextLockFile.set_context(self.context_id)
        await asyncio.sleep(5)

    async def context_routine(self):
        while True:
            if not self._cluster_manager.status == "running":
                self._status = "waiting for the cluster"
                await asyncio.sleep(5)
            else:
                if not self.context_id:
                    await self._creation_routine()
                else:
                    self._status = "verifying if existing context is active"
                    status_info = await self._get_context_status_info()

                    if not status_info:
                        self._status = "no info provided from the existing context"
                        await self._creation_routine()
                    else:
                        current_status = status_info.get("status")

                        if not current_status:
                            self._status = "existing context is not active, creating a new one"
                            await self._creation_routine()
                        else:
                            if current_status == "Running":
                                self._status = "running"
                                await asyncio.sleep(5)
                            else:
                                self._status = "existing context is not active, creating a new one"
                                self.context_id = None
                                await asyncio.sleep(3)
