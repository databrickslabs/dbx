from functools import partial
from pathlib import Path

import pytest

from dbx.api.config_reader import ConfigReader
from dbx.api.destroyer import DestroyerConfig


def test_destroy_models(temp_project):
    config_reader = ConfigReader(Path("conf/deployment.yml"), None)
    config = config_reader.get_config()
    deployment = config.get_environment("default", raise_if_not_found=True)

    base_config = partial(
        DestroyerConfig, workflows_only=False, assets_only=False, dracarys=False, deployment=deployment
    )

    good_config: DestroyerConfig = base_config(
        workflows=[f"{temp_project.name}-sample-etl"],
    )
    assert good_config.workflows == [f"{temp_project.name}-sample-etl"]

    with pytest.raises(ValueError):
        base_config(workflows=["some-non-existent"])

    config_autofill: DestroyerConfig = base_config(workflows=[])
    assert config_autofill.workflows is not None
