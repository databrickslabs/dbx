from functools import partial
from pathlib import Path

import pytest

from dbx.api.config_reader import ConfigReader
from dbx.models.destroyer import DestroyerConfig, DeletionMode


def test_destroy_model(temp_project):
    config_reader = ConfigReader(Path("conf/deployment.yml"), None)
    config = config_reader.get_config()
    deployment = config.get_environment("default", raise_if_not_found=True)
    base_config = partial(DestroyerConfig, deletion_mode=DeletionMode.all, dracarys=False, deployment=deployment)
    good_config: DestroyerConfig = base_config(
        workflow_names=[f"{temp_project.name}-sample-etl"],
    )
    assert good_config.workflow_names == [f"{temp_project.name}-sample-etl"]

    with pytest.raises(ValueError):
        base_config(workflow_names=["some-non-existent"])

    config_autofill: DestroyerConfig = base_config(workflow_names=[])
    assert config_autofill.workflow_names is not None
