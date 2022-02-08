Multitask jobs support
======================

Please note the following - since attribute :code:`libraries` is not supported on the job level in multistep jobs, during deployment the dependencies will be propagated towards every task definition.

You can read more about multistep jobs here (`AWS <https://docs.databricks.com/data-engineering/jobs/index.html>`_, `Azure <https://docs.microsoft.com/en-us/azure/databricks/data-engineering/jobs/>`_, `GCP <https://docs.gcp.databricks.com/data-engineering/jobs/index.html>`_).

Please find some examples for multitask jobs below.

Sample multitask jobs based on Jobs API 2.0
-------------------------------------------

.. tabs::

   .. tab:: JSON

      .. literalinclude:: ../../tests/deployment-configs/03-multitask-job.json
         :language: JSON

   .. tab:: YAML

      .. literalinclude:: ../../tests/deployment-configs/03-multitask-job.yaml
         :language: yaml

Sample multitask jobs based on Jobs API 2.1
-------------------------------------------

Jobs API 2.1 introduces a lot of useful features for job management, and we encourage developers to use this API.
If you would like to enable this API, please do one of the following:

* In case if you're using local Databricks CLI profiles, please follow `this documentation <https://docs.databricks.com/dev-tools/cli/jobs-cli.html#requirements-to-call-the-jobs-rest-api-21>`_
* In your CI pipeline, simply set this environment variable: :code:`export DATABRICKS_JOBS_API_VERSION=2.1` to enable the latest features


.. tabs::

   .. tab:: JSON

      .. literalinclude:: ../../tests/deployment-configs/06-json-jobs-api-2.1-example.json
         :language: JSON

   .. tab:: YAML

      .. literalinclude:: ../../tests/deployment-configs/06-yaml-jobs-api-2.1-example.yaml
         :language: yaml
