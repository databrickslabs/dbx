from enum import Enum


class ExistingRunsOption(str, Enum):
    wait = "wait"
    cancel = "cancel"
    pass_ = "pass"


class IncludeOutputOption(str, Enum):
    stdout = "stdout"
    stderr = "stderr"
