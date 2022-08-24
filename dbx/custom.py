import inspect
from typing import Union, Iterable

import click
from rich.console import group
from rich.markdown import Markdown
from rich.text import Text
from typer.core import MarkupMode
from typer.rich_utils import MARKUP_MODE_MARKDOWN, STYLE_HELPTEXT_FIRST_LINE, _make_rich_rext


@group()
def _get_custom_help_text(
    *,
    obj: Union[click.Command, click.Group],
    markup_mode: MarkupMode,
) -> Iterable[Union[Markdown, Text]]:
    # Fetch and dedent the help text
    help_text = inspect.cleandoc(obj.help or "")

    # Trim off anything that comes after \f on its own line
    help_text = help_text.partition("\f")[0]

    # Get the first paragraph
    first_line = help_text.split("\n\n")[0]
    # Remove single linebreaks
    if markup_mode != MARKUP_MODE_MARKDOWN and not first_line.startswith("\b"):
        first_line = first_line.replace("\n", " ")
    yield _make_rich_rext(
        text=first_line.strip(),
        style=STYLE_HELPTEXT_FIRST_LINE,
        markup_mode=markup_mode,
    )

    # Get remaining lines, remove single line breaks and format as dim
    remaining_paragraphs = help_text.split("\n\n")[1:]
    if remaining_paragraphs:
        remaining_lines = inspect.cleandoc("\n\n".join(remaining_paragraphs).replace("<br/>", "\\"))
        yield _make_rich_rext(
            text=remaining_lines,
            style="cyan",
            markup_mode=markup_mode,
        )
