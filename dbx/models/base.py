from typing import Optional, Dict, Any

from pydantic import BaseModel, root_validator


class FlexibleBaseModel(BaseModel):
    """
    The flexible base model allows undeclared fields to remain in the model definition.
    It allows to avoid modeling every single property and keep consistent support of various API changes.
    """

    extra: Optional[Dict[str, Any]]

    @root_validator(pre=True)
    def build_extra(cls, values: Dict[str, Any]) -> Dict[str, Any]:  # noqa
        all_required_field_names = {field.alias for field in cls.__fields__.values() if field.alias != "extra"}
        extra: Dict[str, Any] = {}
        for field_name in list(values):
            if field_name not in all_required_field_names:
                extra[field_name] = values.pop(field_name)
        values["extra"] = extra
        return values


class CustomProperties(BaseModel):
    disable_core_package_reference: Optional[bool] = False
