Multitask jobs support
======================

Since version 0.2.0 you can also use :code:`dbx` together with multitask job feature.

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

.. tabs::

   .. tab:: JSON

      .. literalinclude:: ../../tests/deployment-configs/06-json-jobs-api-2.1-example.json
         :language: JSON

   .. tab:: YAML

      .. literalinclude:: ../../tests/deployment-configs/06-yaml-jobs-api-2.1-example.yaml
         :language: yaml
