from dbx.api.launch.processors import ClusterReusePreprocessor


def test_preprocessor_positive():
    nc_payload = {"some_key": "some_value"}
    nc_untouched_payload = {"some_key", "some_other_value"}
    test_spec = {
        "job_clusters": [{"job_cluster_key": "main", "new_cluster": nc_payload}],
        "tasks": [
            {"task_key": "some-task", "job_cluster_key": "main"},
            {"task_key": "some-other-task", "new_cluster": nc_untouched_payload},
        ],
    }
    proc = ClusterReusePreprocessor(test_spec)
    result_spec = proc.process()
    assert "job_clusters" not in result_spec
    assert result_spec["tasks"][0]["new_cluster"] == nc_payload
    assert result_spec["tasks"][1]["new_cluster"] == nc_untouched_payload
