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
      libraries: #(5)
        - notebook:
            path: "/Repos/some/path"
        - notebook:
            path: "/Repos/some/other/path"
      target: "some_target_db"
      ... #(7)
```

1. [REQUIRED] If not provided, `dbx` will try to parse the workflow definition as a workflow in Jobs format.
2. [OPTIONAL] A path to a DBFS directory for storing checkpoints and tables created by the pipeline. The system uses a default location if this field is empty.
3. [OPTIONAL] A list of key-value pairs to add to the Spark configuration of the cluster that will run the pipeline.
4. [OPTIONAL] If this is not specified, the system will select a default cluster configuration for the pipeline.
5. Follow documentation for this section [here](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-api-guide.html#pipelinesnewcluster).
6. [REQUIRED] The notebooks containing the pipeline code and any dependencies required to run the pipeline.
7. Follow the [official documentation page](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-api-guide.html#pipelinesettings) for other fields and properties

!!! tip "Payload structure for DLT pipelines"

     In general, `dbx` will use the payload structure specified in the [CreatePipeline](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-api-guide.html#create-a-pipeline) API of DLT.
     All features of `dbx` such as [named properties](../../features/named_properties.md) are fully supported with pipelines deployment as well.

Please note that prior to deploying the pipeline, you'll need to update the relevant notebook sources.

This could be done by using the functionality of the main `databricks-cli`:

```bash
databricks repos update --path="/Repos/some/path" --branch="specific-branch"
databricks repos update --path="/Repos/some/path" --tag="specific-tag"
```

## :material-hexagon-multiple-outline: Launching DLT pipelines using `dbx`


To launch a DLT pipeline, simply use the `dbx launch`command:

```bash
dbx launch <pipeline-name>
```

!!! danger "Assets-based launch is not supported in DLT pipelines"

     Please note that assets-based launch is not supported in DLT pipelines.<br/>
     Use the properties of the DLT pipeline, such as `target` and ` development` if you're looking for CI launch capability.

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
