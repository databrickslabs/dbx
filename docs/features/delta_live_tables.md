# :material-table-heart: Deploying [Delta Live Tables](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-quickstart.html) pipelines

!!! tip "Supported versions"

    Delta Live Tables pipeline support is introduced in `dbx` since version **0.7.5**.<br/>
    If you're using an older version, please upgrade first.


`dbx` provides capabilities for deployment management and launch for Delta Live Tables pipelines.

!!! danger "Interactive development for DLT"

    Please note that `dbx` doesn't provide support for interactive development of DLT. Please use standard notebook-based approach to develop DLT pipelines.

!!! tip "Adding packages to DLT"

    If you would like to develop your package locally and then add it to the DLT, upload your package to DBFS and then reference it in the beginning of the notebook:
    ```python
    %pip install /dbfs/some/package.whl
    ```
    Please note that installation command shall be the **very first** cell in the DLT notebook.

## :material-file-code: Configuring DLT pipelines

`dbx` supports adding a DLT pipeline to your [:material-file-code: deployment file](../reference/deployment.md). Use the following syntax:

```yaml title="conf/deployment.yml"
# some code omitted
environments:
  default:
    workflows:
      - name: "some-pipeline"
        type: "pipeline"
        settings: #(1)
          storage: "/Users/username/data",
          clusters:
            label: "default"
            autoscale:
              min_workers: 1
              max_workers: 5
          libraries:
            - notebook:
                path: "/Users/username/DLT Notebooks/Delta Live Tables quickstart (SQL)"
          continuous: false
```

1. This section shall follow the Delta Live Tables API [`PipelineSettings` object](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-api-guide.html#pipelinesettings)

!!! tip "Using named properties in DLT cluster definitions"

    `dbx` provides convenient name-to-id conversion logic which is described [here](../features/named_properties.md).<br/>
    The following named properties are supported for DLT clusters:

    -   :fontawesome-solid-microchip: `instance_pool_name` will be automatically replaced with `instance_pool_id`
    -   :fontawesome-solid-microchip: `driver_instance_pool_name` will be automatically replaced with `driver_instance_pool_id`
    -   :material-aws: `aws_attributes.instance_profile_name` will be automatically replaced with `aws_attributes.instance_profile_arn`
    -   :material-list-status: `policy_name` will be automatically replaced with `policy_id` and static properties of the policy will be applied to the cluster definition

To deploy the pipeline, use the following command:

```bash
dbx deploy some-pipeline
```

!!! danger "Assets-based deployments and DLT"

    Please note that [assets-based deployment](./assets.md) is **not supported** with DLT pipelines.

To launch the pipeline, use the following command:

```bash
dbx launch some-pipeline
```

!!! danger "Assets-based launch and DLT"

    Please note that [assets-based launch](./assets.md) is **not supported** with DLT pipelines.

There are various parameters for DLT pipelines start as pointed out in [this doc](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-api-guide.html#examples).

You can pass these parameters at the start time in the following way:

```bash
dbx launch some-pipeline --parameters='{"full_refresh": true}' #(1)
dbx launch some-pipeline --parameters='{
  "refresh_selection": ["sales_orders_cleaned", "sales_order_in_chicago"]
}' #(2)
dbx launch some-pipeline --parameters='{
  "refresh_selection": ["sales_orders_cleaned", "sales_order_in_chicago"],
  "full_refresh_selection": ["customers", "sales_orders_raw"]
}' #(3)
```

1. Follow [this doc](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-api-guide.html#start-a-full-refresh) for details
2. Follow [this doc](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-api-guide.html#start-an-update-of-selected-tables) for details
3. Follow [this doc](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-api-guide.html#start-a-full-update-of-selected-tables) for details

## :octicons-workflow-24: Referencing DLT pipelines in the workflow definitions

To reference a pipeline by its name in the workflow definition, please do the following:

```yaml title="conf/deployment.yml"
# some code omitted
environments:
  default:
    workflows:
      - name: "some-pipeline"
        type: "pipeline"
        settings: #(1)
          storage: "/Users/username/data",
          clusters:
            label: "default"
            autoscale:
              min_workers: 1
              max_workers: 5
          libraries:
            - notebook:
                path: "/Users/username/DLT Notebooks/Delta Live Tables quickstart (SQL)"
          continuous: false
      - name: "some-workflow"
        tasks:
            - task_key: "first"
              pipeline_task:
                pipeline_name: "some-pipeline" #(2)
```

1. This section shall follow the Delta Live Tables API [`PipelineSettings` object](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-api-guide.html#pipelinesettings)
2. You can reference pipelines by `pipeline_name` or by `pipeline_id`

!!! danger "Order of definitions"

    Please note that if you reference a DLT pipeline in the workflow definition, you shall add the pipeline deployment definition **before** referencing the pipeline in the `pipeline_task`.


## :material-notebook-heart: Updating notebooks in Repos

The deployed DLT pipeline always points to the set of notebooks defined in the pipeline definitions.
These notebooks are stored in Databricks Repos. Therefore if you would like to automatically update the repo content from your DevOps pipelines, consider the commands below.

To update the specific repo, please use standard Databricks CLI functionality:

```bash
databricks repos update --help # will return arguments and details on the repo updates
```

!!! tip "Using dbx build configuration"

    If you would like to automatically update repos before `dbx deploy`, consider using the [build management](./build_management.md) capabilities of the deployment file.
    For example you can include repo update in the build section using this syntax:
    ```yaml title="conf/deployment.yml"
    # irrelevant content is omitted
    build:
        commands:
            - "databricks repos update --path=/Repos/path/to/repo --branch=some-branch-name"
    ```

