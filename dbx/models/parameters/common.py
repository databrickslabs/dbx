from __future__ import annotations

from typing import Optional, Dict, List, Any

ParamPair = Optional[Dict[str, str]]
StringArray = Optional[List[str]]


def validate_contains(fields: Dict[str, Any], values: Dict[str, Any]):
    _matching_fields = [f for f in fields if f in values]
    if not _matching_fields:
        raise ValueError(f"Provided payload {values} doesn't contain any of the supported fields: {fields}")
    return values


def validate_unique(fields: Dict[str, Any], values: Dict[str, Any]):
    _matching_fields = [f for f in fields if f in values]
    if len(_matching_fields) > 1:
        raise ValueError(f"Provided payload {values} contains more than one definition")

    return values
