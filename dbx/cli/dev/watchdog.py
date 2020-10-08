import asyncio
import datetime as dt

import click
import urwid as ur
from databricks_cli.cli import debug_option
from databricks_cli.clusters.api import ClusterService
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.utils import CONTEXT_SETTINGS

from dbx.cli.execute import _preprocess_cluster_args
from dbx.utils.common import (
    environment_option, dbx_echo, prepare_environment, ApiV1Client, ContextLockFile
)

COMMANDS = {
    'install_libraries': '%pip install pyngrok pathlib',
    'stop_ngrok': """
        import os
        os.system('pkill -f ngrok')
    """,
    'install_ssh_keys': """
        from pathlib import Path
        private_key = b\"\"\"{private_key}\"\"\"
        public_key = b\"\"\"{public_key}\"\"\"
        Path("~/.ssh").expanduser().mkdir(exist_ok=True)
        Path("~/.ssh/id_rsa").expanduser().write_bytes(private_key)
        Path("~/.ssh/id_rsa.pub").expanduser().write_bytes(public_key)
        Path("~/.ssh/authorized_keys").expanduser().write_bytes(public_key)
    """,
    'generate_url': """
        from pyngrok import ngrok
        ngrok.set_auth_token('{token}')
        ssh_url = ngrok.connect(22, "tcp")
    """,
    'print_url': 'print(ssh_url)'
}


class ClusterManager:
    def __init__(self, api_client: ApiClient, cluster_id: str):
        self._api_client = api_client
        self._cluster_service = ClusterService(api_client)
        self.cluster_id = cluster_id
        self._status = "initializing"

    @property
    def status(self):
        return self._status.lower()

    async def cluster_routine(self):
        while True:
            cluster_status = self._cluster_service.get_cluster(self.cluster_id).get("state")
            self._status = cluster_status
            if cluster_status in ["RUNNING", "RESIZING"]:
                await asyncio.sleep(5)
            if cluster_status in ["TERMINATED", "TERMINATING"]:
                self._cluster_service.start_cluster(self.cluster_id)
                await asyncio.sleep(5)
            elif cluster_status == "ERROR":
                raise Exception(
                    "Cluster is mis-configured and cannot be started, please check cluster settings at first")
            elif cluster_status in ["PENDING", "RESTARTING"]:
                await asyncio.sleep(10)


class ContextManager:
    def __init__(self, api_v1_client: ApiV1Client, cluster_manager: ClusterManager):
        self._api_v1_client = api_v1_client
        self._cluster_manager = cluster_manager
        self._status = "initializing"
        self._context_id = ContextLockFile.get_context()

    @property
    def status(self):
        return self._status

    async def _create_context(self, language: str = "python"):
        dbx_echo("Creating context")
        payload = {'language': language, 'clusterId': self._cluster_manager.cluster_id}
        response = self._api_v1_client.create_context(payload)
        return response["id"]

    async def _get_context_status_info(self):
        payload = {"clusterId": self._cluster_manager.cluster_id, "contextId": self._context_id}
        status_info = self._api_v1_client.get_context_status(payload)
        return status_info

    async def context_routine(self):
        while True:
            if not self._cluster_manager.status == "running":
                self._status = "waiting for the cluster"
                await asyncio.sleep(5)
            else:
                if not self._context_id:
                    self._status = "creating a new context"
                    self._context_id = await self._create_context()
                    ContextLockFile.set_context(self._context_id)
                else:
                    self._status = "verifying if existing context is active"
                    status_info = await self._get_context_status_info()

                    if not status_info:
                        self._status = "no info provided from the existing context"
                        self._context_id = None
                        await asyncio.sleep(3)

                    current_status = status_info.get("status")

                    if not current_status:
                        self._status = "existing context is not active, creating a new one"
                        self._context_id = None
                        await asyncio.sleep(3)
                    else:
                        if current_status == "Running":
                            self._status = "running"
                            await asyncio.sleep(5)
                        else:
                            self._status = "existing context is not active, creating a new one"
                            self._context_id = None
                            await asyncio.sleep(3)


class DevApp:
    PALETTE = [
        ('header', 'dark red', ''),
        ('env', 'light green', '')
    ]

    header_widget = ur.Text("")

    cluster_status_widget = ur.Text("")
    context_status_widget = ur.Text("")

    status_widget = ur.Columns([
        ur.LineBox(cluster_status_widget),
        ur.LineBox(context_status_widget),
    ])

    main_widget = ur.Filler(ur.Pile([
        ur.LineBox(header_widget),
        status_widget
    ]), 'bottom')

    def __init__(self,
                 environment: str,
                 cluster_manager: ClusterManager,
                 context_manager: ContextManager,
                 port: int = 4004):
        self._cluster_manager = cluster_manager
        self._context_manager = context_manager
        self._port = port
        self._environment = environment
        self._asyncio_loop = asyncio.get_event_loop()

        self._header_task = self._asyncio_loop.create_task(self._header_handler())

        self._cluster_routine_task = self._asyncio_loop.create_task(self._cluster_manager.cluster_routine())
        self._cluster_status_task = self._asyncio_loop.create_task(self._cluster_status_handler())

        self._context_routine_task = self._asyncio_loop.create_task(self._context_manager.context_routine())
        self._context_status_task = self._asyncio_loop.create_task(self._context_status_handler())

        self._server_coroutine = asyncio.start_server(self._server_routine, '127.0.0.1', 4004, loop=self._asyncio_loop)
        self._server = self._asyncio_loop.run_until_complete(self._server_coroutine)

        self._ur_main_loop = ur.MainLoop(self.main_widget, self.PALETTE,
                                         event_loop=ur.AsyncioEventLoop(loop=self._asyncio_loop))

        self._ur_main_loop.watch_pipe(self._update_header)
        self._ur_main_loop.watch_pipe(self._update_cluster_status)
        self._ur_main_loop.watch_pipe(self._update_context_status)

        self._update_header()
        self._update_cluster_status()
        self._update_context_status()

    async def _server_routine(self, _, reader):
        pass

    async def _header_handler(self):
        while True:
            await asyncio.sleep(0.75)
            self._update_header()

    async def _cluster_status_handler(self):
        while True:
            await asyncio.sleep(5)
            self._update_cluster_status()

    async def _context_status_handler(self):
        while True:
            await asyncio.sleep(5)
            self._update_context_status()

    def _update_context_status(self):
        self.context_status_widget.set_text(f"Context status: {self._context_manager.status}")

    def _update_cluster_status(self):
        self.cluster_status_widget.set_text(f"Cluster status: {self._cluster_manager.status}")

    @staticmethod
    def _current_dttm():
        return " %s" % dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _update_header(self):
        self.header_widget.set_text([('header', "dbx"),
                                     " dev console in ",
                                     ('env', self._environment),
                                     self._current_dttm()
                                     ])

    def launch(self):
        self._ur_main_loop.start()

        try:
            self._asyncio_loop.run_forever()
        except KeyboardInterrupt:
            self._ur_main_loop.stop()
            dbx_echo("Dev server successfully stopped")

        self._header_task.cancel()

        self._cluster_status_task.cancel()
        self._cluster_routine_task.cancel()

        self._context_status_task.cancel()
        self._context_routine_task.cancel()

        self._server.close()

        self._asyncio_loop.run_until_complete(self._server.wait_closed())

        self._asyncio_loop.close()


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Launches local development appliance")
@click.option("--cluster-id", required=False, type=str, help="Cluster ID.")
@click.option("--cluster-name", required=False, type=str, help="Cluster name.")
@environment_option
@debug_option
def watchdog(environment: str,
             cluster_id: str,
             cluster_name: str):
    dbx_echo("Starting watchdog in environment %s" % environment)

    api_client = prepare_environment(environment)
    api_v1_client = ApiV1Client(api_client)

    cluster_id = _preprocess_cluster_args(api_client, cluster_name, cluster_id)

    cluster_manager = ClusterManager(api_client, cluster_id)
    context_manager = ContextManager(api_v1_client, cluster_manager)

    app = DevApp(environment, cluster_manager, context_manager)
    app.launch()
