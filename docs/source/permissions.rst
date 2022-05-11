Job permissions management
==========================

With :code:`dbx` you can manage permissions of your job during the :code:`dbx deploy` step.
Please note that :code:`dbx` uses `Permissions API <https://docs.databricks.com/dev-tools/api/latest/permissions.html>`_ under the hood, so your permission settings should follow that API description.

We're using the :code:`PUT` method of `this API method <https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/update-all-job-permissions>`_ to make the deployment process consistent.

To enable permission settings during deploy, simply add :code:`"permissions"` section into your job definition. Please note that payload under this section shall be compliant with Permissions API.

.. tabs::

   .. tab:: JSON

      .. literalinclude:: ../../tests/deployment-configs/09-permissions.json
         :language: JSON
            
   .. tab:: YAML

      .. literalinclude:: ../../tests/deployment-configs/09-permissions.yaml
         :language: yaml
 
Note that the **access control list** must be exhaustive, so the job owner should be added to the list as well as added users/user groups permissions.
