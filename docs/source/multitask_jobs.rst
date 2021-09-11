Multitask jobs support
======================

Since version 0.2.0 you can also use :code:`dbx` together with `multitask jobs <https://docs.databricks.com/data-engineering/jobs/index.html>`_.

Please note the following - since attribute :code:`libraries` is not supported on the job level in multistep jobs, during deployment the dependencies will be propagated towards every task definition.

You can read more about multistep jobs here (`AWS <https://docs.databricks.com/data-engineering/jobs/index.html>`_,`Azure <https://docs.microsoft.com/en-us/azure/databricks/data-engineering/jobs/>`_, `GCP <https://docs.gcp.databricks.com/data-engineering/jobs/index.html>`_.

Here are some examples for multitask job definitions:

.. tabs::

   .. tab:: JSON

      .. literalinclude:: ../../tests/deployment-configs/03-multitask-job.json
         :language: JSON

   .. tab:: YAML

      .. literalinclude:: ../../tests/deployment-configs/03-multitask-job.yaml
         :language: JSON
