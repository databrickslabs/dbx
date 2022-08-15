import typer
from dbx import __version__


def version_callback(value: bool):
    if value:
        typer.echo(f"DataBricks eXtensions aka dbx, version ~> {__version__}")
        raise typer.Exit()


def main(
    version: bool = typer.Option(None, "--version", callback=version_callback, is_eager=True),
):
    pass
