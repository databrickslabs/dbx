from typing import Optional

from pydantic import BaseModel


class DbxDeploymentConfig(BaseModel):
    no_package: Optional[bool] = False
