import os

import click
from cryptography.hazmat.backends import default_backend as crypto_default_backend
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from databricks_cli.clusters.api import ClusterService
from databricks_cli.utils import CONTEXT_SETTINGS
from databricks_cli.cli import debug_option
import pathlib
from dbx.cli.execute import _preprocess_cluster_args, awake_cluster, get_context_id, execute_command
from dbx.utils.common import (
    environment_option, dbx_echo, prepare_environment, ApiV1Client, ContextLockFile,
    TunnelInfo, get_ssh_client
)
from dbx.utils.watchdog import Rsync
from typing import Tuple
import paramiko
import sys
import asyncio
import datetime as dt
import urwid as ur
import time

from databricks_cli.sdk.api_client import ApiClient

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
        self._cluster_id = cluster_id

    async def status(self):
        return self._cluster_service.get_cluster(self._cluster_id).get("state").lower()

    async def _awake_cluster(self):
        cluster_status = await self.status()
        if cluster_status in ["RUNNING", "RESIZING"]:
            dbx_echo("Cluster is ready")
        if cluster_status in ["TERMINATED", "TERMINATING"]:
            dbx_echo("Dev cluster is terminated, starting it")
            self._cluster_service.start_cluster(self._cluster_id)
            await asyncio.sleep(5)
            await self._awake_cluster()
        elif cluster_status == "ERROR":
            raise Exception("Cluster is mis-configured and cannot be started, please check cluster settings at first")
        elif cluster_status in ["PENDING", "RESTARTING"]:
            dbx_echo("Cluster is getting prepared, current state: %s" % cluster_status)
            await asyncio.sleep(10)
            await self._awake_cluster()


class ContextManager:
    def __init__(self, cluster_manager: ClusterManager, v1_client: ApiV1Client, cluster_id: str):
        self._cluster_manager = cluster_manager
        self._v1_client = v1_client
        self._cluster_id = cluster_id
        self._context_id = ContextLockFile.get_context()

    async def status(self):
        is_available = await self._is_context_available()
        if is_available:
            return "available"
        else:
            return "preparing"

    async def _is_context_available(self) -> bool:
        if not self._context_id:
            return False
        else:
            payload = {"clusterId": self._cluster_id, "contextId": self._context_id}
            resp = self._v1_client.get_context_status(payload)
            if not resp:
                return False
            elif resp.get("status"):
                return resp["status"] == "Running"

    async def _get_context_id(self):
        dbx_echo("Preparing execution context")

        is_available = await self._is_context_available()

        if is_available:
            dbx_echo("Existing context is active, using it")
            return self._context_id
        else:
            dbx_echo("Existing context is not active, creating a new one")
            context_id = await self._create_context()
            ContextLockFile.set_context(context_id)
            dbx_echo("New context prepared, ready to use it")
            self._context_id = context_id

    async def _create_context(self, language: str = "python"):
        payload = {'language': language, 'clusterId': self._cluster_id}
        response = self._v1_client.create_context(payload)
        return response["id"]


class DevApp:
    PALETTE = [
        ('header', 'dark red', '')
    ]

    header_widget = ur.Text("")

    cluster_status_widget = ur.Text("")
    context_status_widget = ur.Text("")
    tunnel_status_widget = ur.Text("")

    status_widget = ur.Columns([
        ur.LineBox(cluster_status_widget),
        ur.LineBox(context_status_widget),
        ur.LineBox(tunnel_status_widget)
    ])

    main_widget = ur.Filler(ur.Pile([
        ur.LineBox(header_widget),
        status_widget
    ]), 'top')

    def __init__(self,
                 cluster_manager: ClusterManager,
                 context_manager: ContextManager,
                 port: int = 4004):
        self._cluster_manager = cluster_manager
        self._context_manager = context_manager
        self._port = port
        self._asyncio_loop = asyncio.get_event_loop()

        self._header_task = self._asyncio_loop.create_task(self._header_handler())
        self._cluster_status_task = self._asyncio_loop.create_task(self._cluster_status_handler())
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

    def _update_cluster_status(self):
        self.cluster_status_widget.set_text(f"Cluster status: {self._cluster_manager.status()}")

    def _update_context_status(self):
        self.context_status_widget.set_text(f"Context status: {self._context_manager.status()}")

    def _update_header(self):
        self.header_widget.set_text([('header', "dbx"),
                                     " dev console @ http://localhost:4004  %s" % dt.datetime.now().strftime(
                                         "%Y-%m-%d %H:%M:%S")])

    def launch(self):
        self._ur_main_loop.start()

        try:
            self._asyncio_loop.run_forever()
        except KeyboardInterrupt:
            self._ur_main_loop.stop()
            print("Dev server successfully stopped")

        self._header_task.cancel()
        self._cluster_status_task.cancel()
        self._context_status_task.cancel()

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
    check_ngrok_env()
    dbx_echo("Starting watchdog in environment %s" % environment)

    api_client = prepare_environment(environment)
    cluster_id = _preprocess_cluster_args(api_client, cluster_name, cluster_id)

    api_v1_client = ApiV1Client(api_client)

    cluster_manager = ClusterManager(api_client, cluster_id)
    context_manager = ContextManager(cluster_manager, api_v1_client, cluster_id)
    app = DevApp(cluster_manager, context_manager)
    app.launch()

    # tunnel_manager = TunnelManager(v1_client, cluster_id, context_id)
    #
    # tunnel_manager.provide_tunnel()
    # tunnel_info = tunnel_manager.get_tunnel_info()
    # dbx_echo(f"Tunnel created via: {tunnel_info.host}:${tunnel_info.port}")
    # rsync = Rsync(tunnel_info)
    # rsync.launch()


def check_ngrok_env():
    if not os.environ.get("DBX_NGROK_TOKEN"):
        dbx_echo("""Current implementation of dbx dev supports only ngrok-based tunnels.
        please obtain a token from ngrok.com and set it as an environment variable:
            export DBX_NGROK_TOKEN='{your token here}' 
        """)
        sys.exit(1)


class TunnelManager:

    def __init__(self, v1_client: ApiV1Client, cluster_id: str, context_id: str):
        self._v1_client = v1_client
        self._cluster_id = cluster_id
        self._context_id = context_id
        self._url = ContextLockFile.get_url()
        self._private_key_file = pathlib.Path("~/.ssh/%s" % self._cluster_id).expanduser()

    def _exec(self, cmd, verbose=False):
        return execute_command(self._v1_client, self._cluster_id, self._context_id, cmd, verbose)

    def get_tunnel_info(self) -> TunnelInfo:
        (host, port) = self._url.replace('tcp://', "").split(":")
        return TunnelInfo(host, int(port), str(self._private_key_file))

    def _prepare_sshd(self):
        client = get_ssh_client(self.get_tunnel_info())
        client.exec_command("mkdir -p /usr/lib/ssh")
        client.exec_command("ln -s /usr/lib/openssh/sftp-server /usr/lib/ssh/sftp-server")
        client.exec_command("systemctl restart ssh.service")

    def initialize_tunnel(self):
        dbx_echo("Initializing a tunnel")
        self._exec(COMMANDS["install_libraries"])
        self._exec(COMMANDS["stop_ngrok"])

        private_key, public_key = self.generate_key_pair()

        if self._private_key_file.exists():
            self._private_key_file.unlink()

        self._private_key_file.write_bytes(private_key)
        os.chmod(self._private_key_file, 0o600)

        remote_keys_cmd = COMMANDS['install_ssh_keys'].format(
            private_key=private_key.decode('utf-8'),
            public_key=public_key.decode('utf-8')
        )

        self._exec(remote_keys_cmd, verbose=False)
        self._exec(COMMANDS['generate_url'].format(token=os.environ["DBX_NGROK_TOKEN"]))

        url = self._exec(COMMANDS['print_url'])
        ContextLockFile.set_url(url)
        self._url = url
        self._prepare_sshd()

    def provide_tunnel(self):
        if not self._url:
            dbx_echo("No existing tunnel metadata found, initializing a new one")
            self.initialize_tunnel()
        else:
            dbx_echo("Found existing tunnel metadata")
            try:
                dbx_echo("Checking if tunnel is still available")
                self.check_tunnel()
                dbx_echo("Existing tunnel is available")
            except (paramiko.ssh_exception.AuthenticationException,
                    paramiko.ssh_exception.NoValidConnectionsError,
                    FileNotFoundError):
                dbx_echo("Tunnel is no more available, initializing a new one")
                self.initialize_tunnel()

    def check_tunnel(self):
        client = get_ssh_client(self.get_tunnel_info())
        client.exec_command("ls -la")

    @staticmethod
    def generate_key_pair() -> Tuple[bytes, bytes]:
        key = rsa.generate_private_key(
            backend=crypto_default_backend(),
            public_exponent=65537,
            key_size=2048
        )

        private_key = key.private_bytes(
            crypto_serialization.Encoding.PEM,
            crypto_serialization.PrivateFormat.TraditionalOpenSSL,
            crypto_serialization.NoEncryption()
        )

        public_key = key.public_key().public_bytes(
            crypto_serialization.Encoding.OpenSSH,
            crypto_serialization.PublicFormat.OpenSSH
        )
        return private_key, public_key
