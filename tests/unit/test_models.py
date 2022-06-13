import pathlib
from dbx.api.readers import ConfigProvider
from dbx.models.tasks import TaskDefinition


def test_base_with_extras():
    td = TaskDefinition(**{"task_key": "some", "max_retries": 10})
    assert td.extra == {"max_retries": 10}


def test_sample_models():
    example_model = pathlib.Path("../deployment-configs/03-multitask-job.yaml")
    env = ConfigProvider(example_model).get_environment("default")

