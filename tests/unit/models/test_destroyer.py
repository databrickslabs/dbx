from pathlib import Path

from dbx.api.config_reader import ConfigReader
from dbx.models.cli.destroyer import DestroyerConfig, DeletionMode


def test_destroy_model(temp_project):
    config_reader = ConfigReader(Path("conf/deployment.yml"), None)
    config = config_reader.get_config()
    deployment = config.get_environment("default", raise_if_not_found=True)
    selected_wfs = [deployment.payload.workflows[0]]
    base_config = DestroyerConfig(
        workflows=selected_wfs, deletion_mode=DeletionMode.all, dracarys=False, deployment=deployment
    )
    assert base_config.workflows == selected_wfs
