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
from dbx.utils.common import environment_option, dbx_echo, prepare_environment, ApiV1Client, ContextLockFile, dbx_log, \
    TunnelInfo, get_ssh_client
from utils.watchdog import Rsync
from typing import Tuple
import paramiko
import sys

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


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Launches development appliance")
@click.option("--cluster-id", required=False, type=str, help="Cluster ID.")
@click.option("--cluster-name", required=False, type=str, help="Cluster name.")
@environment_option
@debug_option
def dev(environment: str,
        cluster_id: str,
        cluster_name: str):

    check_ngrok_env()

    dbx_echo("Starting development appliance in environment %s" % environment)
    api_client = prepare_environment(environment)
    cluster_id = _preprocess_cluster_args(api_client, cluster_name, cluster_id)
    cluster_service = ClusterService(api_client)
    dbx_echo("Preparing cluster to accept jobs")
    awake_cluster(cluster_service, cluster_id)

    v1_client = ApiV1Client(api_client)
    context_id = get_context_id(v1_client, cluster_id, "python")

    tunnel_manager = TunnelManager(v1_client, cluster_id, context_id)

    tunnel_manager.provide_tunnel()

    tunnel_info = tunnel_manager.get_tunnel_info()
    rsync = Rsync(tunnel_info)
    rsync.launch()


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
        dbx_log("Preparing sshd config")
        client = get_ssh_client(self.get_tunnel_info())
        client.exec_command("mkdir -p /usr/lib/ssh")
        client.exec_command("ln -s /usr/lib/openssh/sftp-server /usr/lib/ssh/sftp-server")
        client.exec_command("systemctl restart ssh.service")
        dbx_log("Preparing sshd config - done")

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
        dbx_log(f"Providing tunnel with existing tunnel url {self._url}")
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
