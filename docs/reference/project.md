# :material-file-cog-outline: Project file reference

Project file is located in the `.dbx/project.json` folder.

This file can be edited manually, but we **strictly** recommend changing configurations only by using `dbx configure` command.

!!! danger

    It's very important to keep the project file in the git repository.
    Don't delete it and don't add it to the `.gitignore`.

Project file stores environment configurations.
Each environment represents one workspace (however you can map same workspaces to different names).

Typical layout of a project file looks like this:

```json title=".dbx/project.json" hl_lines="3 5 6-8 12"
{
    "environments": {
        "default": {
            "profile": "charming-aurora",
            "storage_type": "mlflow",
            "properties": {
                "workspace_directory": "/Shared/dbx/charming_aurora",
                "artifact_location": "dbfs:/Shared/dbx/projects/charming_aurora"
            }
        }
    },
    "inplace_jinja_support": true
}
```

The `default` line defines the name of the environment. This name shall be then used in [:material-file-code: deployment file](./deployment.md).

**Storage type** specifies the type of artifact storage.
Artifact storage is used to store file references and other dependent configurations.
At the moment `dbx` only supports `mlflow` as artifact storage.

Profile line specifies the local Databricks CLI profile that would be used for deployments.

!!! tip

    In CI and CD pipelines there is no need to define the profile.<br/>
    Simply use `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables.
    These variables will take precedence over the `profile` definition.


Behaviour of the `inplace_jinja_support` is described in the [Jinja support doc](../features/jinja_support.md).
