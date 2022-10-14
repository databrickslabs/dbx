from dbx.models.workflow.common.parameters import ParametersMixin, NamedParametersMixin


class ExecuteParametersPayload(ParametersMixin, NamedParametersMixin):
    """Parameters for execute"""
