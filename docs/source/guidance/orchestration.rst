Integration with orchestration tools
====================================

Integration with Azure Data Factory
-----------------------------------

To perform integration with Azure Data Factory, please do the following steps:

* Please ensure that pipeline is created and published in Azure Data Factory.

1. Deploy from cli command

* Deploy the latest job version and write deployment result into a file

.. code-block::

    dbx deploy --write-specs-to-file=.dbx/deployment-result.json --files-only

* Reflect job definitions to Azure Data Factory activities:

.. code-block::

    dbx datafactory reflect \
        --specs-file=.dbx/deployment-result.json \
        --subscription-name some-subscription \
        --resource-group some-group \
        --factory-name some-factory \
        --name some-pipeline-name
        
2. Deploy from CI/CD pipeline

Inside your CI/CD pipeline, add Azure login before the ``dbx datafactory reflect`` step. Below is an example using GitHub Actions:

.. code-block::

    - name: Deploy and write deployment result into a file
      run: |
        dbx deploy --deployment-file ./conf/deployment_single_task.yml --write-specs-to-file=./.dbx/deployment-result.json --files-only
    
    - name: Azure Login
      uses: azure/login@v1
      with:
        creds: ${{ secrets.AZURE_CREDENTIALS }}

    - name: Reflect job definitions to ADF
      run: |
        dbx datafactory reflect \
        --specs-file=.dbx/deployment-result.json \
        --subscription-name some-subscription \
        --resource-group some-group \
        --factory-name some-factory \
        --name some-pipeline-name        

See more details on Azure login `here`_

.. _here: https://github.com/marketplace/actions/azure-login#configure-a-service-principal-with-a-secret)

This command will create or update linked services and pipeline activities. Each job will be configured as a separate activity.


.. warning::

    Please note following limitations of this approach:
     * runs triggered from Azure Data Factory won't be mentioned in the job runs
     * changing job definition manually in Databricks UI won't change the properties of ADF-defined activities
     * only Python-based activities are supported at this moment
     * MSI authentication is not yet supported
     * :code:`policy_id` argument is not yet supported (it will be ignored during deployment to ADF)
     * | Manual changes to the parameters of Databricks tasks will be nullified during next reflect.
       | All parameters for Databricks tasks shall be provided in the deployment file
     * Multi-task jobs are not supported by ADF


Integration with Apache Airflow
-------------------------------

To trigger job execution from Apache Airflow, please do the following:

* Deploy jobs to Databricks:

.. code-block::

    dbx deploy

* Add this function to get job id by the job name into your Airflow setup:

.. code-block:: python

    from airflow.contrib.hooks.databricks_hook import DatabricksHook

    def get_job_id_by_name(job_name: str, databricks_conn_id: str) -> str:
        list_endpoint = ('GET', 'api/2.0/jobs/list')
        hook = DatabricksHook(databricks_conn_id=databricks_conn_id)
        response_payload = hook._do_api_call(list_endpoint, {})
        all_jobs = response_payload.get("jobs", [])
        matching_jobs = [j for j in all_jobs if j["settings"]["name"] == job_name]

        if not matching_jobs:
            raise Exception(f"Job with name {job_name} not found")

        if len(matching_jobs) > 1:
            raise Exception(f"Job with name {job_name} is duplicated. Please make job name unique in Databricks UI.")

        job_id = matching_jobs[0]["job_id"]
        return job_id

* Use this function from your DAG:

.. code-block:: python

    from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

    job_id = get_job_id_by_name("some-job-name", "some-databricks-conn-id")
    operator = DatabricksRunNowOperator(
        job_id=job_id,
        # add your arguments
    )
