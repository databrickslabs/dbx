# :material-file-code: Deployment file reference

Deployment file is one of the most important files for `dbx` functionality.

## :material-format-columns: File format options
It contains workflows definitions, as well as `build` configurations.

The following file extensions are supported:

- `.yml` and `.yaml` expect a YAML-like payload. You can enable inplace Jinja support as described [here](../features/jinja_support.md)
- `.json` expects a JSON-like payload. You can enable inplace Jinja support as described [here](../features/jinja_support.md)
- `.yml.j2` and `.yaml.j2` will expect a YAML-like payload with possible sections of [Jinja blocks](../features/jinja_support.md).
- `.json.j2` will expect a JSON-like payload with possible sections of [Jinja blocks](../features/jinja_support.md).

By default `dbx` commands will search for a `deployment.*` file in the `conf` directory of the project.
Alternatively, all commands that require a deployment file support passing it explicitly via `--deployment-file` option.

## :material-page-layout-header-footer: Layout

Typical layout of the deployment file looks like this:

```yaml title="conf/deployment.yml"

build: #(1)
  python: "pip"

environments: #(2)
  default: #(3)
    workflows: #(4)
      - name: "workflow1" #(5)
        tasks:
          - task_key: "task1"
            # example task payload
            python_wheel_task:
              package_name: "some-pkg"
              entry_point: "some-ep"
```

1. Read more on the topic of build management [here](../features/build_management.md)
2. This section is **required**. Without it `dbx` won't be able to read the file.
3. This is the name of a specific environment. This environment shall exist in [project file](./project.md)
4. This section is **required**. Without it `dbx` won't be able to read the workflows definitions.
5. Workflow names shall be unique.

!!! tip

    As the project file, deployment file supports multiple environments.
    You can configure them by naming new environments under the `environments` section.

The `workflows` section of the deployment file fully follows the [Databricks Jobs API structures](https://docs.databricks.com/dev-tools/api/latest/jobs.html) with features described in [this section](../features/assets.md).

## :material-alpha-t-box-outline: Various workflow types

<img src="https://img.shields.io/badge/available%20since-0.8.0-green?style=for-the-badge" alt="Available since 0.8.0"/>

`dbx` supports the following workflow types:

- Workflows in [Jobs API :material-surround-sound-2-0: format](https://docs.databricks.com/dev-tools/api/2.0/jobs.html)
- Workflows in [Jobs API :material-surround-sound-2-1: format](https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate)
- Workflows in [:material-table-heart: Delta Live Tables pipeline format](https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate)

If the `workflow_type` property is not specified on the workflow level, `dbx` will define the type based on the following rules:

- If `tasks` section is provided, then the workflow type is `jobs-v2.1`.
- If this section is not provided, workflow will be parsed as a `jobs-v2.0`.

Allowed values for the `workflow_type` field are:

- `jobs-v2.1`
- `jobs-v2.0`
- `pipeline`

Examples below demonstrate how to define various workflow types:

```yaml title="conf/deployment.yml"

build:
  python: "pip"

environments:
  default:
    workflows:

      ################################################
      - name: "workflow-in-v2.1-format"
        tasks:
          - task_key: "task1"
            python_wheel_task:
              package_name: "some-pkg"
              entry_point: "some-ep"

      ################################################
      - name: "workflow-in-v2.0-format"
        spark_python_task:
          python_file: "file://some/file.py"

      ################################################
      - name: "workflow-in-pipeline-format"
        target: "some-target-db"
        workflow_type: "pipeline" # enforces the recognition
        libraries:
          - notebook:
              path: "/Repos/some/path"
```

!!! tip "DLT with `dbx` guide"

    Read more on the topic of DLT pipelines with `dbx` [here](../guides/general/delta_live_tables.md).

## :material-package-up: Advanced package dependency management

By default `dbx` is heavily oriented towards Python package-based projects. However, for pure Notebook or JVM projects this might be not necessary.

Therefore, to disable the default behaviour of `dbx` which tries to add the Python package dependency, use the `deployment_config` section inside the task definition:

=== "Latest - Jobs API :material-surround-sound-2-1:"

    ```yaml title="conf/deployment.yml" hl_lines="12-13"
      # some code omitted
      environments:
        default:
          workflows:
            - name: "workflow1"
              tasks:
                - task_key: "task1"
                  python_wheel_task: #(1)
                    package_name: "some-pkg"
                    entry_point: "some-ep"
                - task_key: "task2" #(2)
                  deployment_config:
                      no_package: true
                  notebook_task:
                      notebook_path: "/some/notebook/path"
    ```

    1. Standard Python package-based payload, the python wheel dependency will be added by default
    2. In the notebook task, the Python package is not required since code is delivered together with the Notebook.
       Therefore, we disable this behaviour by providing this property.

=== "Legacy - Jobs API :material-surround-sound-2-0:"

    ```yaml title="conf/deployment.yml" hl_lines="10-11"
      # some code omitted
      environments:
        default:
          workflows:
            - name: "wheel-workflow"
              python_wheel_task: #(1)
                package_name: "some-pkg"
                entry_point: "some-ep"
            - name: "notebook-workflow" #(2)
              deployment_config:
                no_package: true
              notebook_task:
                notebook_path: "/some/notebook/path"
    ```

    1. Standard Python package-based payload, the python wheel dependency will be added by default
    2. In the notebook task, the Python package is not required since code is delivered together with the Notebook.
       Therefore, we disable this behaviour by providing this property.

## :material-folder-star-multiple: Examples

This section contains various examples of the deployment file for various cases.
Most of the examples below use inplace Jinja functionality which is [described here](../features/jinja_support.md#enable-inplace-jinja).

### :material-tag-plus: Tagging workflows

To tag the workflow for better UI experience use the following structure:

```yaml title="conf/deployment.yml" hl_lines="6-9"

# some code omitted
environments:
  default:
    workflows:
      - name: "workflow1"
        tags:
         some_tag: "tag-value"
         some_other_tag: "another-tag-value"
```

!!! danger "Note about `--tags` parameter in `dbx deploy` and `dbx launch`"

    Please note that `--tags` parameter in the `dbx deploy` and `dbx launch` command **are not relevant** to the workflow tags.
    Read more in the relevant command documentation.


### :material-timer: Scheduling workflows

To schedule the workflow, use the `schedule` section.

```yaml title="conf/deployment.yml" hl_lines="6-9"

# some code omitted
environments:
  default:
    workflows:
      - name: "workflow1"
        schedule:
         quartz_cron_expression: "0 0 * * *" #(1)
         timezone_id: "Europe/Berlin" #(2)
```

1. This sets up the schedule for every day at midnight. Check [this site](http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html) for more examples.
2. Timezone is set accordingly to the Java [`TimeZone`](https://docs.oracle.com/javase/7/docs/api/java/util/TimeZone.html) class.

!!! tip "Official Databricks docs"

    More profound doc about schedule section could be [found here](https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate**).

!!! tip "External orchestration"

    If you're using an external scheduler or orchestrator, you can also easily orchestrate Databricks workflows from it.
    Here are some examples for [Apache Airflow](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/connections/databricks.html) and [Prefect](https://docs-v1.prefect.io/api/0.15.13/tasks/databricks.html).


### :octicons-zap-24: Enabling Photon

To define job clusters with [Photon](https://www.databricks.com/product/photon) support, add the following to the configuration:

```yaml title="conf/deployment.yaml" hl_lines="9"
custom:
  basic-cluster-props: &basic-cluster-props
    spark_version: "your-spark-version"
    node_type_id: "your-node-type-id"
    spark_conf:
      spark.databricks.delta.preview.enabled: 'true'
    instance_pool_name: <enter pool name>
    driver_instance_pool_name: <enter pool name>
    runtime_engine: PHOTON
    init_scripts:
      - dbfs:
        destination: dbfs:/<enter your path>

```

### :material-dots-hexagon: Managing the workflow as a service principal

<img src="https://img.shields.io/badge/available%20since-0.8.0-green?style=for-the-badge" alt="Available since 0.8.0"/>

This example uses the [named reference feature](../features/named_properties.md#reference-based-approach):

```yaml title="conf/deployment.yml" hl_lines="5-7"
environments:
  default:
    workflows:
      - name: "example-workflow"
        access_control_list:
         - service_principal_name: "service-principal://some-service-principal-name"
           permission_level: "IS_OWNER"
         - user_name: "some-real-user@email.com"
           permission_level: "CAN_MANAGE"
```

### :material-code-array: Configuring complex deployments

While configuring complex deployments, it's recommended to use YAML anchor mechanics to avoid repeating code blocks.

Here is a very detailed example of a complex deployment:

```yaml title="conf/deployment.yaml"
custom:
  basic-cluster-props: &basic-cluster-props
    spark_version: "your-spark-version"
    node_type_id: "your-node-type-id"
    spark_conf:
      spark.databricks.delta.preview.enabled: 'true'
    instance_pool_id: "instance-pool://some-pool-name"
    driver_instance_pool_id: "instance-pool://some-pool-name"
    runtime_engine: STANDARD
    init_scripts:
      - dbfs:
          destination: dbfs:/<enter your path>

  basic-auto-scale-props: &basic-auto-scale-props
    autoscale:
      min_workers: 2
      max_workers: 4

  basic-static-cluster: &basic-static-cluster
    new_cluster:
      <<: *basic-cluster-props
      num_workers: 2

  basic-autoscale-cluster: &basic-autoscale-cluster
    new_cluster:
      <<: # merge these two maps and place them here.
        - *basic-cluster-props
        - *basic-auto-scale-props

environments:
  default:
    workflows:
      - name: "your-job-name"

        email_notifications:
          on_start: [ "user@email.com" ]
          on_success: [ "user@email.com" ]
          on_failure: [ "user@email.com" ]

        no_alert_for_skipped_runs: false

        schedule:
          quartz_cron_expression: "00 25 03 * * ?" #(1)
          timezone_id: "UTC"
          pause_status: "PAUSED"

        tags:
          your-key: "your-value"
          your-key1: "your-value1"

        format: MULTI_TASK #(2)

        access_control_list:
          - user_name: "service-principal://some-service-principal"
            permission_level: "IS_OWNER"
          - group_name: "your-group-name"
            permission_level: "CAN_VIEW"

        job_clusters:
          - job_cluster_key: "basic-cluster"
            <<: *basic-static-cluster
          - job_cluster_key: "basic-autoscale-cluster"
            <<: *basic-autoscale-cluster

        tasks:
          - task_key: "your-task-01"
            job_cluster_key: "basic-cluster"
            max_retries: 1
            spark_python_task:
              python_file: "file://sample_project/jobs/your-file-01.py"

            min_retry_interval_millis: 900000
            retry_on_timeout: false
            timeout_seconds: 0
            email_notifications:
              on_start:
                - user@email.com
              on_success:
                - user@email.com
              on_failure:
                - user1@email.com
                - user2@email.com

          - task_key: "your-task-02"
            job_cluster_key: "basic-cluster"
            spark_python_task:
              python_file: "file://sample_project/jobs/your-file-02.py"
            depends_on:
              - task_key: "your-task-01"

          - task_key: "your-task-03"
            job_cluster_key: "basic-cluster"
            notebook_task:
              notebook_path: "/Repos/some/project/notebook"
            depends_on:
              - task_key: "your-task-01"

          - task_key: "example_sql_task"
            job_cluster_key: "basic-cluster"
            sql_task:
              query: "query://some-query-name"
              warehouse_id: "warehouse://some-warehouse-id"

          - task_key: "example_dbt_task"
            job_cluster_key: "basic-cluster"
            dbt_task:
              project_directory: "/some/project/dir"
              profiles_directory: "/some/profiles/dir"
              warehouse_id: "warehouse://some-warehouse-id"
              commands:
                - "dbt cmd1"
                - "dbt cmd2"
```

1. Read more about scheduling in [this section](./deployment.md#scheduling-workflows)
2. `format` key is not required, but you can provide it to explicitly enforce the job format. Please follow the [Jobs API doc](https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate) for details.
