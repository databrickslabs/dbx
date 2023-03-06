# :material-code-brackets: Passing parameters

<img src="https://img.shields.io/badge/available%20since-0.8.0-green?style=for-the-badge" alt="Available since 0.8.0"/>


`dbx` provides various interfaces to pass the parameters to the workflows and tasks.
Unfortunately, the underlying APIs and the fact that `dbx` is a CLI tool are somewhat
limiting the capabilities of parameter passing and require additional payload preparation.

This documentation section explains various ways to pass parameters both statically and dynamically.


## :material-pin-outline: Static parameter passing


To pass the parameters statically, you shall use the deployment file.
Please find reference on various tasks and their respective parameters in the [deployment file reference](../../reference/deployment.md).


## :material-clipboard-flow-outline: Dynamic parameter passing

In some cases you would like to override the parameters that are defined in the static file to launch the workflow.

Therefore, there are 3 fundamentally different options on how parameters could be provided dynamically:

- Parameters of a **:octicons-package-dependents-24: task** in [`dbx execute`](../../reference/cli.md#dbx-execute)
- Parameters of a **:octicons-workflow-24: workflow** in the [asset-based launch mode](../../features/assets.md) (`dbx launch --from-assets`)
- Parameters of a **:octicons-workflow-24: workflow** in the normal launch mode (`dbx launch`)

All parameters should be provided in a form of a JSON-compatible payload (see examples below).

!!! tip "Multiline strings in the shell"

    To handle a multiline string in the shell, use single quotes (`'`), for instance:

    ```bash
    dbx ... --parameters='{
      "nicely": "formatted"
      "multiline": [
        "string",
        "with",
        "many",
        "rows"
      ]
    }'
    ```

=== ":material-lightning-bolt-circle: `dbx execute`"

    The `execute` command expects the parameters only for one specific task (if it's a multitask job) or only for the job itself (if it's the old 2.0 API format).
    The provided payload shall match the expected payload of a chosen workflow or task.
    It also should be wrapped as a JSON-compatible string with curly brackets around the API-compatible payload.
    Since `dbx execute` only supports `spark_python_task` and `python_wheel_task`, only the compatible parameters would work.

    Examples:
    ```bash
    dbx execute <workflow_name> --parameters='{"parameters": ["argument1", "argument2"]}' # compatible with spark_python_task and python_wheel_task
    dbx execute <workflow_name> --parameters='{"named_parameters": {"a":1, "b": 2}}' # compatible only with python_wheel_task
    ```

=== ":material-lightning-bolt-outline: `dbx launch --assets-only`"

    !!! tip "General note about 2.0 and 2.1 Jobs API"

        We strongly recommend using the Jobs API 2.1, since it provides extensible choice of tasks and options.

    Assets-based launch provides 2 different interfaces depending on the Jobs API version you use.

    For Jobs API 2.0, the payload format should be compliant with the [RunSubmit](https://docs.databricks.com/dev-tools/api/2.0/jobs.html#runs-submit) API structures in terms of the task arguments.

    Examples for Jobs API 2.0:
    ```bash
    dbx launch <workflow_name> --assets-only --parameters='{"base_parameters": {"key1": "value1", "key2": "value2"}}' # for notebook_task
    dbx launch <workflow_name> --assets-only --parameters='{"parameters": ["argument1", "argument2"]}' # for spark_jar_task, spark_python_task and spark_submit_task
    ```

    For Jobs API 2.1, the payload format should be compliant with the [JobsRunSubmit](https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit).
    This API supports task-level parameter passing.

    Examples for Jobs API 2.1:

    ```bash
    # notebook_task
    dbx launch <workflow_name> --assets-only <workflow_name> --parameters='[
        {"task_key": "some", "base_parameters": {"a": 1, "b": 2}}
    ]'

    # spark_python_task, python_wheel_task, spark_jar_task, spark_submit_task, python_wheel_task
    dbx launch <workflow_name> --assets-only <workflow_name> --parameters='[
        {"task_key": "some", "parameters": ["a", "b"]}
    ]'

    # python_wheel_task
    dbx launch <workflow_name> --assets-only <workflow_name> --parameters='[
        {"task_key": "some", "named_parameters": {"a": 1, "b": 2}}
    ]'

    # pipeline_task
    dbx launch <workflow_name> --assets-only <workflow_name> --parameters='[
        {"task_key": "some", "full_refresh": true}
    ]'

    # sql_task
    dbx launch <workflow_name> --assets-only <workflow_name> --parameters='[
        {"task_key": "some", "parameters": {"key1": "value2"}}
    ]'
    ```


=== ":material-lightning-bolt-outline: `dbx launch`"

    !!! tip "General note about 2.0 and 2.1 Jobs API"

        We strongly recommend using the Jobs API 2.1, since it provides extensible choice of tasks and options.

    For Jobs API 2.0, the payload format should be compliant with the [RunNow](https://docs.databricks.com/dev-tools/api/2.0/jobs.html#run-now) API structures in terms of the task arguments.

    Examples:

    ```bash
    dbx launch <workflow_name> --parameters='{"jar_params": ["a1", "b1"]}' # spark_jar_task
    dbx launch <workflow_name> --parameters='{"notebook_params":{"name":"john doe","age":"35"}}' # notebook_task
    dbx launch <workflow_name> --parameters='{"python_params":["john doe","35"]}' # spark_python_task
    dbx launch <workflow_name> --parameters='{"spark_submit_params": ["--class", "org.apache.spark.examples.SparkPi"]}' # spark_submit_task
    ```

    For Jobs API 2.1, the payload format should be compliant with the [RunNow](https://docs.databricks.com/dev-tools/api/2.0/jobs.html#run-now) API structures in terms of the task arguments.

    !!! danger "Per-task parameter passing is not supported"

        Unfortunately it's not possible to pass parameters to the each task individually.

        As a workaround, you can name parameters differently in different tasks.

        It is  also possible to provide a combined payload, e.g. in you have a `notebook_task` and a `spark_python_task` in your workflow, you can combine as follows:
        ```bash
        dbx launch <workflow_name> --parameters='{
            "notebook_params":{"name":"john doe","age":"35"},
            "python_params":["john doe","35"]
        }'
        ```

    Examples of parameter provisioning:
    ```bash
    dbx launch <workflow_name> --parameters='{"jar_params": ["a1", "b1"]}' # spark_jar_task
    dbx launch <workflow_name> --parameters='{"notebook_params":{"name":"john doe","age":"35"}}' # notebook_task
    dbx launch <workflow_name> --parameters='{"python_params":["john doe","35"]}' # spark_python_task or python_wheel_task
    dbx launch <workflow_name> --parameters='{"spark_submit_params": ["--class", "org.apache.spark.examples.SparkPi"]}' # spark_submit_task
    dbx launch <workflow_name> --parameters='{"python_named_params": {"name": "task", "data": "dbfs:/path/to/data.json"}}' # python_wheel_task
    dbx launch <workflow_name> --parameters='{"pipeline_params": {"full_refresh": true}}' # pipeline_task as a part of a workflow
    dbx launch <workflow_name> --parameters='{"sql_params": {"name": "john doe", "age": "35"}}' # sql_task
    dbx launch <workflow_name> --parameters='{"dbt_commands": ["dbt deps", "dbt seed", "dbt run"]}' # dbt_task
    ```

!!! tip "Passing parameters for DLT pipelines"

    Please follow the [Delta Live Tables guide](./delta_live_tables.md) to pass parameters that are specific for DLT pipelines.
