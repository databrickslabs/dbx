import textwrap


class ValidationError(ValueError):
    """
    Custom validation error class which provides nice formatting for multiline strings
    """

    def __init__(self, message):
        _message = textwrap.fill(textwrap.dedent(message))
        super(ValidationError, self).__init__(_message)
