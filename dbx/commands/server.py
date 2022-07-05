import click
import uvicorn
from dbx.server.server import app, APP_HOST, APP_PORT


@click.group(help="dbx driver server commands")
def server():
    pass


@click.command(help="starts the dbx driver server")
def start():
    uvicorn.run(app, port=APP_PORT, host=APP_HOST)


server.add_command(start, "start")
