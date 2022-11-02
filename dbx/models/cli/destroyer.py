from enum import Enum
from typing import Optional, List

from pydantic import BaseModel

from dbx.models.deployment import EnvironmentDeploymentInfo, AnyWorkflow


class DeletionMode(str, Enum):
    all = "all"
    assets_only = "assets-only"
    workflows_only = "workflows-only"


class DestroyerConfig(BaseModel):
    workflows: Optional[List[AnyWorkflow]] = []
    deletion_mode: DeletionMode
    dry_run: Optional[bool] = False
    dracarys: Optional[bool] = False
    deployment: EnvironmentDeploymentInfo
