# :material-table-heart: Working with Delta Live Tables

<img src="https://img.shields.io/badge/available%20since-0.8.0-green?style=for-the-badge" alt="Available since 0.8.0"/>

`dbx` provides capabilities to deploy, launch and reference pipelines based on the [Delta Live Tables framework](https://docs.databricks.com/workflows/delta-live-tables/index.html).

!!! warning "Development support for DLT Pipelines"

    Please note that `dbx` doesn't provide interactive development and execution capabilities for DLT Pipelines.

## :material-hexagon-multiple-outline: Providing pipeline definition in the deployment file

Example pipeline definition would look like this:

```yaml title="conf/deployment.yml"
environments:
  workflows:
    - name: "sample-dlt-pipeline"
      workflow_type: "pipeline" #(1)
      storage: "dbfs:/some/location" #(2)
      configuration: #(3)
        "property1": "value"
        "property2": "value2"
      clusters: #(4)
        - label: "some-label" #(5)
          spark_conf:
            "spark.property1": "value"
            "spark.property2": "value2"
          aws_attributes:
            ...
          instance_pool_id: "instance-pool://some-pool"
          driver_instance_pool_id: "instance-pool://some-pool"
          policy_id: "cluster-policy://some-policy"
          autoscale:
            min_workers: 1
            max_workers: 4
            mode: "legacy" #(6)
      libraries: #(7)
        - notebook:
            path: "/Repos/some/path"
        - notebook:
            path: "/Repos/some/other/path"
      target: "some_target_db"
      ... #(8)
```

1. [REQUIRED] If not provided, `dbx` will try to parse the workflow definition as a workflow in Jobs format.
2. [OPTIONAL] A path to a DBFS directory for storing checkpoints and tables created by the pipeline. The system uses a default location if this field is empty.
3. [OPTIONAL] A list of key-value pairs to add to the Spark configuration of the cluster that will run the pipeline.
4. [OPTIONAL] If this is not specified, the system will select a default cluster configuration for the pipeline.
5. Follow documentation for this section [here](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-api-guide.html#pipelinesnewcluster).
6. Also, could be `mode: "enchanced"`, read more on [this feature here](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-concepts.html#databricks-enhanced-autoscaling).
7. [REQUIRED] The notebooks containing the pipeline code and any dependencies required to run the pipeline.
8. Follow the [official documentation page](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-api-guide.html#pipelinesettings) for other fields and properties

!!! tip "Payload structure for DLT pipelines"

     In general, `dbx` will use the payload structure specified in the [CreatePipeline](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-api-guide.html#create-a-pipeline) API of DLT.
     All features of `dbx` such as [named properties](../../features/named_properties.md) are fully supported with pipelines deployment as well.

Please note that prior to deploying the pipeline, you'll need to update the relevant notebook sources.

This could be done by using the functionality of the main `databricks-cli`:

```bash
databricks repos update --path="/Repos/some/path" --branch="specific-branch"
databricks repos update --path="/Repos/some/path" --tag="specific-tag"
```

## :material-rocket-launch: Launching DLT pipelines using `dbx`


To launch a DLT pipeline, simply use the `dbx launch` command with `-p` or `--pipeline` switch:

```bash
dbx launch <pipeline-name> -p # also could be --pipeline instead of -p
```

!!! danger "Assets-based launch is not supported in DLT pipelines"

     Please note that [assets-based launch](../../features/assets.md) is **not supported for DLT pipelines**.

     Use the properties of the DLT pipeline, such as `target` and `development` if you're looking for capabilities to launch a specific branch.

## :material-code-brackets: Passing parameters to DLT pipelines during launch

Following the API structures and examples provided [here](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-api-guide.html#start-a-pipeline-update):

```bash
dbx launch <pipeline-name> --parameters='{ "full_refresh": "true" }' # for full refresh
dbx launch <pipeline-name> --parameters='{ "refresh_selection": ["sales_orders_cleaned", "sales_order_in_chicago"] }' # start an update of selected tables
dbx launch <pipeline-name> --parameters='{
  "refresh_selection": ["sales_orders_cleaned", "sales_order_in_chicago"],
  "full_refresh_selection": ["customers", "sales_orders_raw"]
}' # start a full update of selected tables
```

## :material-link-plus: Referencing DLT pipelines inside multitask workflows

Sometimes you might need to chain various tasks around the DLT pipeline. In this case you can use the `pipeline_task` capabilities of the Databricks Workflows.
For example, your deployment file definition could look like this:

```yaml title="conf/deployment.yml" hl_lines="21-22"
environments:
  default:
    workflows:
      - name: "some-pipeline"
        workflow_type: "pipeline"
        libraries:
          - notebook:
              path: "/Repos/some/project"
      - name: "dbx-pipeline-chain"
        job_clusters:
          - job_cluster_key: "main"
            <<: *basic-static-cluster
        tasks:
          - task_key: "one"
            python_wheel_task:
              entry_point: "some-ep"
              package_name: "some-pkg"
          - task_key: "two"
            depends_on:
              - task_key: "one"
            pipeline_task:
              pipeline_id: "pipeline://some-pipeline" #(1)
```

1. Read more on the reference syntax [here](../../features/named_properties.md).

!!! tip "Order of the definitions"

    Please note that if you're planning to reference the pipeline definition, you should explicitly put them first in the deployment file `workflows` section, **prior to the reference**.

    Without the proper order you'll run into a situation where your DLT pipeline wasn't yet deployed, therefore the reference won't be resolved which will cause an error.
