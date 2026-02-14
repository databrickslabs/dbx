from typing import Optional

from pydantic.v1 import BaseModel


class DbxDeploymentConfig(BaseModel):
    no_package: Optional[bool] = False
