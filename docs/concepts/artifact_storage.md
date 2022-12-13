# :material-diamond-stone: Artifact storage

To properly resolve the [file references](../features/file_references.md) and store the artifacts (e.g. packages, files and deployment definitions),
`dbx` uses persistent cloud storage which is called **artifact storage**.

## :material-bookshelf: Storage configuration
To perform upload/download/lookup operations, `dbx` uses MLflow APIs under the hood.
Currently, `dbx` only supports MLflow-based API for file operations.

MLflow-based artifact storage properties are specified per environment in the [project configuration file](../reference/project.md).

When project is configured by default, the definition looks like this:

```json title="project.json" hl_lines="6-8"
{
    "environments": {
        "default": {
            "profile": "some-profile",
            "storage_type": "mlflow",
            "properties": {
                "workspace_directory": "/Shared/dbx/some-project-name",
                "artifact_location": "dbfs:/Shared/dbx/projects/some-project-name"
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

    **By default, any file stored in the `dbfs://` non-mounted location could be accessed in R/W mode by any user of the same workspace.**

    Therefore, we recommend storing your deployment artifacts in `s3://`, `wasbs://` or `gs://`-based artifact locations.
    By doing so you'll ensure that only the relevant people will be able to work with this location.

!!! tip "Using a non-`dbfs` based artifact location with `dbx execute`"

    <img src="https://img.shields.io/badge/available%20since-0.8.0-green?style=for-the-badge" alt="Available since 0.8.0"/>

    Since `dbx execute` expects from the artifact location FUSE support, this limitation could be overcome by using context-based loading.

    To enable context-based loading, you can either specify a project-wide property by configuring it:
    ```bash
    dbx configure --enable-context-based-upload-for-execute
    ```
    Alternatively, you can specift it per each execution by using `dbx execute` with `--upload-via-context` switch.


!!! info "ADLS resolution specifics"

    Since `mlfow` only supports `wasbs://`-based paths, and Databricks API requires job objects to be referenced via `abfss://`,
    during the file reference resolution `dbx` will use the `wasbs://` protocol for uploads but references will be still resolved into `abfss://` format.


## :material-book-plus: Additional libraries

<img src="https://img.shields.io/badge/available%20since-0.8.8-green?style=for-the-badge" alt="Available since 0.8.8"/>

In case if you're using a cloud-based storage, you might require additional libraries to be installed.

Add the following extra to your `pip install dbx[chosen-identifier]`:

- :material-microsoft-azure: for `wasbs://` use `dbx[azure]`
- :material-aws: for `s3://` use `dbx[aws]`
- :material-google-cloud: for `gs://` use `dbx[gcp]`


