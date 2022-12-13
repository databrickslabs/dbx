from dbx.api.launch.processors import ClusterReusePreprocessor
from dbx.models.workflow.v2dot1.workflow import Workflow


def test_preprocessor_positive():
    nc_payload = {"some_key": "some_value", "spark_version": "some-version"}
    nc_untouched_payload = {"some_key": "some-other-value", "spark_version": "some-version"}
    test_spec = {
        "name": "name",
        "job_clusters": [{"job_cluster_key": "main", "new_cluster": nc_payload}],
        "tasks": [
            {"task_key": "some-task", "job_cluster_key": "main", "spark_python_task": {"python_file": "here.py"}},
            {
                "task_key": "some-other-task",
                "new_cluster": nc_untouched_payload,
                "spark_python_task": {"python_file": "here.py"},
            },
        ],
    }
    wf = Workflow(**test_spec)
    proc = ClusterReusePreprocessor()
    proc.process(wf)
    assert wf.job_clusters is None
    assert wf.get_task("some-task").new_cluster.dict(exclude_none=True) == nc_payload
    assert wf.get_task("some-other-task").new_cluster.dict(exclude_none=True) == nc_untouched_payload
