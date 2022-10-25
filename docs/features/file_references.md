# :material-link-plus: File reference management

During the workflow deployment, you frequently would like to upload some specific files to `DBFS` and reference them in
the workflow definition.

## :material-file-link: File referencing by example

Any keys referenced in the deployment file starting with `file://` or `file:fuse://` will be uploaded to the artifact
storage.

References are resolved with relevance to the root of the project.

Imagine the following project structure:

```shell title="project structure"
.
├── charming_aurora #
│   ├── __init__.py
│   ├── common.py
│   └── tasks
│       ├── __init__.py
│       ├── sample_etl_task.py
│       └── sample_ml_task.py
├── conf
│   ├── deployment.yml
│   └── tasks
│       ├── sample_etl_config.yml
│       └── sample_ml_config.yml
```

In your [:material-file-code: deployment file](../reference/deployment.md) you can specify the file references in the
following way:

```yaml title="conf/deployment.yml" hl_lines="11-14"
environments:
  default:
    workflows:
      - name: "charming-aurora-sample-multitask"
        job_clusters:
          - job_cluster_key: "default"
            <<: *basic-static-cluster
        tasks:
          - task_key: "etl"
            job_cluster_key: "default"
            spark_python_task:
              python_file: "file://charming_aurora/tasks/sample_etl_task.py"
              parameters: [ "--conf-file", "file:fuse://conf/tasks/sample_etl_config.yml" ]
```

As you can see there are two **different** prefixes for file references.

## :material-file-search: FUSE and standard reference resolution

There are two types of how the file path will be resolved and referenced in the final deployment definition:

=== "Standard"

    This definition looks like this `file://some/path/in/project/some.file`.<br/>
    It will be resolved into `<artifact storage prefix>/some/path/in/project/some.file`.

    Files that were referenced in a standard way are in most cases used as workflow properties (e.g. init scripts or `spark_python_file` references).
    To programmatically read a file that was referenced in standard way, you'll need to use object storage compatible APIs, for example Spark APIs:
    ```python
    standard_referenced_path = "dbfs://some/path" # or "s3://some/path" or "gs://some/path" or "abfss://some/path"

    def read_text_payload(_path):
        return "\n".join(spark.read.format("text").load(standard_referenced_path).select("value").toPandas()["value"])
    raw_text_payload = read_text_payload(standard_referenced_path)
    ```

=== "FUSE"

    This definition looks like this `file:fuse://some/path/in/project/some.file`.<br/>
    It will be resolved into `/dbfs/<artifact storage prefix>/some/path/in/project/some.file`.

    In most cases FUSE-based paths are used to pass something into a library which only supports reading files from local FS.

    For instance, a use-case might be to read a configuration file using Python `pathlib` library:
    ```python
    from pathlib import Path
    fuse_based_path = "/dbfs/some/path"
    payload = Path(fuse_based_path).read_text()
    ```
    Although FUSE is a very convenient approach, unfortunately it only works with `dbfs://`-based artifact locations (both mounted and non-mounted).

!!! tip "Various artifact storage types process references differently"

    Please read the [artifact storage](../concepts/artifact_storage.md) for details on how various references would work with different artifact storage types.
