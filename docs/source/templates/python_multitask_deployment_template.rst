Multi Task Deployment.yaml Template
===================================

.. code-block:: bash

  .
  ├── .dbx
  │   ├── lock.json  # please note that this file shall be ignored and not added to your git repository.
  │   └── project.json
  ├── .gitignore
  ├── README.md
  ├── conf
  │   ├── deployment.yml
  │   └── test
  │       └── sample.yml
  ├── pytest.ini
  ├── sample_project
  │   ├── __init__.py # <- this is the root folder of your Python package
  │   ├── common.py # <- this file contains a generic class called Job, which provides you all necessary tools, such as Spark and DBUtils
  │   └── jobs
  │       ├── __init__.py
  │       ├── your-file-01.py
  │       └── your-file-02.py
  ├── setup.py
  ├── tests
  │   ├── integration
  │   │   └── sample_test.py
  │   └── unit
  │       └── sample_test.py
  └── requirements.txt


**To get list of spark-versions**

:code:`$ databricks clusters spark-versions`

**To get list of node-type-ids**

:code:`$ databricks clusters list-node-types`

**Note**

Documentation tries to list all the options, based on your need some options may not be relevant.
Example: If pool_id is used, then note_type_id may not be relevant and so on.

.. code-block:: yaml

  custom:
  basic-cluster-props: &basic-cluster-props
      spark_version: "your-spark-version"
      node_type_id: "your-node-type-id"
      spark_conf:
        spark.databricks.delta.preview.enabled: 'true'
      instance_pool_id: <enter pool id>
      driver_instance_pool_id: <enter pool id>
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
      requirements: file://requirements.txt

  basic-autoscale-cluster: &basic-autoscale-cluster
      new_cluster:
      <<: # merge these two maps and place them here.
          - *basic-cluster-props
          - *basic-auto-scale-props

  environments:
  myjob:
    strict_path_adjustment_policy: true
    jobs:
    - name: "your-job-name"
        email_notifications:
        on_start: ["user@email.com"]
        on_success: ["user@email.com"]
        on_failure: ["user@email.com"]

        #http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html
        schedule:
        quartz_cron_expression: "00 25 03 * * ?"
        timezone_id: "UTC"
        pause_status: "PAUSED" #

        tags:
        your-key: "your-value"
        your-key1: "your-value1"

        permissions:
          access_control_list:
            - user_name: "user@email.com"
              permission_level: "IS_OWNER"
            #- group_name: "your-group-name"
            #permission_level: "CAN_VIEW"
            #- user_name: "user2@databricks.com"
            #permission_level: "CAN_VIEW"
            #- user_name: "user3@databricks.com"
            #permission_level: "CAN_VIEW"

        job_clusters:
        - job_cluster_key: "basic-cluster"
            <<: *basic-static-cluster

        tasks:
        - task_key: "your-task-01"
            job_cluster_key: "basic-cluster"
            max_retries: 1
            spark_python_task:
            python_file: "file://sample_project/jobs/your-file-01.py"
        - task_key: "your-task-02"
            job_cluster_key: "basic-cluster"
            spark_python_task:
            python_file: "file://sample_project/jobs/your-file-02.py"
            depends_on:
            - task_key: "your-task-01"

**Create the Job from CLI**

:code:`dbx deploy --environment=myjob --no-rebuild`

**Run the Job Manually from CLI**

:code:`dbx launch --environment=myjob --job=demo_pyspark_job``

