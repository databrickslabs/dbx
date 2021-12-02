Passing environment variables to the deployment configuration
=============================================================

:code:`dbx` supports passing environment variables into the deployment configuration, giving you an additional level of flexibility.
You can pass environment variables both into JSON and YAML-based configurations. This allows you to parametrize the deployment and make it more flexible for CI pipelines.

.. tabs::

   .. tab:: JSON

      .. literalinclude:: ../../tests/deployment-configs/04-json-with-env-vars.json
         :language: JSON

   .. tab:: YAML

      .. literalinclude:: ../../tests/deployment-configs/04-yaml-with-env-vars.yaml
         :language: YAML


We also support specifying default values with environment variables. They should be specified like :code:`${ENV_VAR:<default_value>}`.

.. note::

    Unlike JSON, in YAML you have to specify the :code:`!ENV` tag before your environment variables for it to be resolved in
    a valid manner.
