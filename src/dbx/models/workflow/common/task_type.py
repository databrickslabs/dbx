from enum import Enum


class TaskType(str, Enum):
    # task types defined both in v2.0 and v2.1
    notebook_task = "notebook_task"
    spark_jar_task = "spark_jar_task"
    spark_python_task = "spark_python_task"
    spark_submit_task = "spark_submit_task"
    pipeline_task = "pipeline_task"

    # specific to v2.1
    python_wheel_task = "python_wheel_task"
    sql_task = "sql_task"
    dbt_task = "dbt_task"

    # undefined handler for cases when a new task type is added
    undefined_task = "undefined_task"
