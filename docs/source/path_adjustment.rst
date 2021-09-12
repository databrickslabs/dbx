Path adjustment logic during deployment
=======================================


During deployment, :code:`dbx` supports uploading local files and properly referencing them in the job definition.
By default, any files referenced in the deployment file (file path shall be relative to the project root) will be uploaded to the dbfs and properly referenced in the job definition.

However, this logic might be too wide defined in some particular cases. To make it more compliant and reliable,
since version 0.2.0 :code:`dbx deploy` supports strict path adjustment policy. To enable this policy, add the following property to the environment:


.. tabs::

   .. tab:: JSON

      .. literalinclude:: ../../tests/deployment-configs/04-path-adjustment-policy.json
         :language: JSON

   .. tab:: YAML

      .. literalinclude:: ../../tests/deployment-configs/04-path-adjustment-policy.yaml
         :language: yaml

Since 0.2.0 :code:`dbx` also supports FUSE-based path replacement (instead of :code:`dbfs:/...` a FUSE-based path will be provided :code:`/dbfs/`).

