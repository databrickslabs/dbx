import asyncio
import datetime as dt
import logging

import urwid as ur
from dbx.utils.common import dbx_echo
from dbx.utils.watchdog.cluster_manager import ClusterManager
from dbx.utils.watchdog.context_manager import ContextManager
from dbx.utils.watchdog.tunnel_manager import TunnelManager
from dbx.utils.watchdog.sync_manager import SyncManager


class DevApp:
    PALETTE = [
        ('header', 'dark red', ''),
        ('env', 'light green', '')
    ]

    header_widget = ur.Text("")

    cluster_status_widget = ur.Text("")
    context_status_widget = ur.Text("")
    tunnel_status_widget = ur.Text("")
    sync_status_widget = ur.Text("")

    status_widget = ur.Pile([
        ur.LineBox(cluster_status_widget),
        ur.LineBox(context_status_widget),
        ur.LineBox(tunnel_status_widget),
        ur.LineBox(sync_status_widget),
    ])

    main_widget = ur.Filler(ur.Pile([
        ur.LineBox(header_widget),
        status_widget
    ]), 'top')

    def __init__(self,
                 environment: str,
                 cluster_manager: ClusterManager,
                 context_manager: ContextManager,
                 tunnel_manager: TunnelManager,
                 sync_manager: SyncManager,
                 port: int = 4004):

        self._cluster_manager = cluster_manager
        self._context_manager = context_manager
        self._tunnel_manager = tunnel_manager
        self._sync_manager = sync_manager

        self._port = port
        self._environment = environment
        self._asyncio_loop = asyncio.get_event_loop()

        self._header_task = self._asyncio_loop.create_task(self._header_handler())

        self._cluster_routine_task = self._asyncio_loop.create_task(self._cluster_manager.cluster_routine())
        self._context_routine_task = self._asyncio_loop.create_task(self._context_manager.context_routine())
        self._tunnel_routine_task = self._asyncio_loop.create_task(self._tunnel_manager.tunnel_routine())
        self._sync_routine_task = self._asyncio_loop.create_task(self._sync_manager.sync_routine())

        self._cluster_status_task = self._asyncio_loop.create_task(self._cluster_status_handler())
        self._context_status_task = self._asyncio_loop.create_task(self._context_status_handler())
        self._tunnel_status_task = self._asyncio_loop.create_task(self._tunnel_status_handler())
        self._sync_status_task = self._asyncio_loop.create_task(self._sync_status_handler())

        self._server_coroutine = asyncio.start_server(self._server_routine, '127.0.0.1', 4004, loop=self._asyncio_loop)
        self._server = self._asyncio_loop.run_until_complete(self._server_coroutine)

        self._ur_main_loop = ur.MainLoop(self.main_widget, self.PALETTE,
                                         event_loop=ur.AsyncioEventLoop(loop=self._asyncio_loop))

        self._ur_main_loop.watch_pipe(self._update_header)
        self._ur_main_loop.watch_pipe(self._update_cluster_status)
        self._ur_main_loop.watch_pipe(self._update_context_status)
        self._ur_main_loop.watch_pipe(self._update_tunnel_status)
        self._ur_main_loop.watch_pipe(self._update_sync_status)

        self._update_header()
        self._update_cluster_status()
        self._update_context_status()
        self._update_tunnel_status()
        self._update_sync_status()

    async def _server_routine(self, _, reader):
        pass

    async def _header_handler(self):
        while True:
            await asyncio.sleep(0.1)
            self._update_header()

    async def _cluster_status_handler(self):
        while True:
            await asyncio.sleep(2)
            self._update_cluster_status()

    async def _context_status_handler(self):
        while True:
            await asyncio.sleep(2)
            self._update_context_status()

    async def _tunnel_status_handler(self):
        while True:
            await asyncio.sleep(2)
            self._update_tunnel_status()

    async def _sync_status_handler(self):
        while True:
            await asyncio.sleep(0.5)
            self._update_sync_status()

    def _update_context_status(self):
        self.context_status_widget.set_text(f"Context status: {self._context_manager.status}")

    def _update_cluster_status(self):
        self.cluster_status_widget.set_text(f"Cluster status: {self._cluster_manager.status}")

    def _update_tunnel_status(self):
        if self._tunnel_manager.status == "running":
            info = self._tunnel_manager.get_tunnel_info()
            msg = f"Tunnel status: {self._tunnel_manager.status} @ {info.host}:{info.port}"
            self.tunnel_status_widget.set_text(msg)
        else:
            self.tunnel_status_widget.set_text(f"Tunnel status: {self._tunnel_manager.status}")

    def _update_sync_status(self):
        self.sync_status_widget.set_text(f"Sync status: {self._sync_manager.status}")

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

        for t in asyncio.tasks.all_tasks(self._asyncio_loop):
            t.cancel()

        self._server.close()

        self._asyncio_loop.run_until_complete(self._server.wait_closed())

        self._asyncio_loop.close()
