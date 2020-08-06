import click

from databrickslabs_cicdtemplates import __version__


def print_version_callback(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return

    output_string = """DataBricks eXtensions [dbx] version ~> %s""" % __version__
    click.echo(output_string)
    ctx.exit()
