Path adjustment logic during deployment
=======================================


During deployment, :code:`dbx` supports uploading local files and properly referencing them in the job definition.
Any keys referenced in the deployment file starting with :code:`file://` or :code:`file:fuse://` will be uploaded to the artifact storage.
References are resolved with relevance to the root of the project.

There are two types of how the file path will be resolved and referenced in the final deployment definition:

* **Standard** - This definition looks like this :code:`file://some/path/in/project/some.file`. This definition will be resolved into :code:`dbfs://<artifact storage prefix>/some/path/in/project/some.file`
* **FUSE** - This definition looks like this :code:`file:fuse://some/path/in/project/some.file`. This definition will be resolved into :code:`/dbfs/<artifact storage prefix>/some/path/in/project/some.file`

The latter type of path resolution might come in handy when the using system doesn't know how to work with cloud storage protocols.

Please find more examples on path resolution below:

.. tabs::

   .. tab:: JSON

      .. literalinclude:: ../../tests/deployment-configs/04-path-adjustment-policy.json
         :language: JSON

   .. tab:: YAML

      .. literalinclude:: ../../tests/deployment-configs/04-path-adjustment-policy.yaml
         :language: yaml

