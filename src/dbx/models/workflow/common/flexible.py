from typing import List

from pydantic import BaseModel, Extra

from dbx.utils import dbx_echo


class FlexibleModel(BaseModel, extra=Extra.allow):
    """
    Base class for models used across all domain objects in dbx.
    Provides extensible functions for verification.
    """

    @classmethod
    def get_field_names(cls) -> List[str]:
        return list(cls.__fields__.keys())

    @classmethod
    def field_deprecated(cls, field_id: str, field_name: str, reference: str, value):
        dbx_echo(
            f"""[yellow]⚠️ Field [bold]{field_name}[/bold] is DEPRECATED.[/yellow]
        Please use the in-place reference instead:
        [code]{field_id}: "{reference}://{value}"[/code]
        """
        )
