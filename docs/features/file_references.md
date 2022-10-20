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

## :material-diamond-stone: Artifact storage types

Currently, `dbx` only supports MLflow-based API for file operations.

Therefore, it's required to have two main properties of the environment in the [project configuration file](../reference/project.md):

When project is configured by default, the definition looks like this:
```json title="project.json" hl_lines="6-8"
{
    "environments": {
        "default": {
            "profile": "some-profile",
            "storage_type": "mlflow",
            "properties": {
                "workspace_directory": "/Shared/dbx/some-project-name", #(1)
                "artifact_location": "dbfs:/Shared/dbx/projects/some-project-name" #(2)
            }
        }
    }
}
```

1. Workspace directory points to an MLflow experiment which will be used as a basis for Mlflow-based operations.
   There is no need to create a new experiment before running any `dbx` commands. Please note that for security purposes to protect this experiment use the experiment permissions model.
2. Artifact location is one of the locations supported by MLflow on Databricks, namely: `dbfs://` (both mounts and root container are supported), `s3://`, `wasbs://` and `gs://`.

!!! warning "Security of the experiment files and the artifact storage"

    To ensure protected R/W access to the deployed objects we **recommend** using `s3://` or `wasbs://` or `gs://` artifact locations.

    By default, any file stored in such `dbfs://` location could be accessed in R/W mode by any user of the same workspace.

    Therefore, we recommend storing your deployment artifacts in `s3://`, `wasbs://` or `gs://`-based artifact locations.
    By doing so you'll ensure that only the relevant people will be able to work with this location.

!!! tip "Using a non-`dbfs` based artifact location with `dbx execute`"

    Since `dbx execute` expects from the artifact location FUSE support, this limitation could be overcome by using context-based loading.

    To enable context-based loading, you can either specify a project-wide property by configuring it:
    ```bash
    dbx configure --enable-context-based-upload-for-execute
    ```
    Alternatively, you can specift it per each execution by using `dbx execute` with `--upload-via-context` switch.


!!! info "ADLS resolution specifics"

    Since `mlfow` only supports `wasbs://`-based paths, and Databricks API requires job objects to be referenced via `abfss://`,
    during the file reference resolution `dbx` will use the `wasbs://` protocol for uploads but references will be still resolved into `abfss://` format.
