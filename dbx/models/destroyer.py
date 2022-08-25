from enum import Enum
from typing import Optional, List

from pydantic import BaseModel, root_validator

from dbx.models.deployment import EnvironmentDeploymentInfo


class DeletionMode(str, Enum):
    all = "all"
    assets_only = "assets-only"
    workflows_only = "workflows-only"


class DestroyerConfig(BaseModel):
    workflows: Optional[List[str]]
    deletion_mode: DeletionMode
    dry_run: Optional[bool] = False
    dracarys: Optional[bool] = False
    deployment: EnvironmentDeploymentInfo

    @root_validator()
    def validate_all(cls, values):  # noqa
        _dc = values["deployment"]
        if not values["workflows"]:
            values["workflows"] = [w["name"] for w in _dc.payload.workflows]
        else:
            _ws_names = [w["name"] for w in _dc.payload.workflows]
            for w in values["workflows"]:
                if w not in _ws_names:
                    raise ValueError(f"Workflow name {w} not found in {_ws_names}")
        return values
