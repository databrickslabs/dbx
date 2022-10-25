from typing import Dict, Any, List


def at_least_one_by_suffix(suffix: str, values: Dict[str, Any]):
    _matching_fields = [f for f in values if f.endswith(suffix)]
    if not _matching_fields:
        raise ValueError(
            f"""
                At least one field with suffix {suffix} should be provided.
                Provided payload: {values}
            """,
        )
    return values


def only_one_by_suffix(suffix: str, values: Dict[str, Any]):
    _matching_fields = [f for f in values if f.endswith(suffix)]

    if len(_matching_fields) != 1:
        _filtered_values = {k: v for k, v in values.items() if v is not None}
        raise ValueError(
            f"""
                Only one field with suffix {suffix} should be provided.
                Provided payload: {_filtered_values}
            """,
        )
    return values


def at_least_one_of(fields_names: List[str], values: Dict[str, Any]):
    """
    Verifies that provided payload contains at least one of the fields
    :param fields_names: List of the field names to be validated
    :param values: Raw payload values
    :return: Nothing, raises an error if validation didn't pass.
    """
    _matching_fields = [f for f in fields_names if f in values]
    if not _matching_fields:
        raise ValueError(
            f"""
            At least one of the following fields should be provided in the payload: {fields_names}.
            Provided payload: {values}
        """,
        )
    return values


def only_one_provided(suffix: str, values: Dict[str, Any]):
    """Function verifies if value IS provided and it's unique"""
    at_least_one_by_suffix(suffix, values)
    only_one_by_suffix(suffix, values)
    return values


def mutually_exclusive(fields_names: List[str], values: Dict[str, Any]):
    non_empty_values = [key for key, item in values.items() if item]  # will coalesce both checks for None and []
    _matching_fields = [f for f in fields_names if f in non_empty_values]
    if len(_matching_fields) > 1:
        raise ValueError(
            f"""
            The following fields {_matching_fields} are mutually exclusive.
            Provided payload: {values}
        """,
        )
    return values


def named_parameters_check(values: List[str]) -> List[str]:
    for element in values:
        if "=" not in element or not element.startswith("--"):
            raise ValueError(
                f"""
            Element {element} in the payload: {values} doesn't contain an equals sign.

            Named parameters should be supplied in the format of "--param1=value"
            """
            )
    return values


def check_dbt_commands(commands):
    if commands:
        for cmd in commands:
            if not cmd.startswith("dbt"):
                raise ValueError("All commands in the dbt_task must start with `dbt`, e.g. `dbt command1`")
    return commands
