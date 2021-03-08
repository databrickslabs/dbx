Setting job permissions during deployment
=========================================

With :code:`dbx` you can manage permissions of your job during the :code:`dbx deploy` step.
Please note that :code:`dbx` uses `Permissions API <https://docs.databricks.com/dev-tools/api/latest/permissions.html>`_ under the hood, so your permission settings should follow that API description.

We're using the :code:`PUT` method of `this API method <https://docs.databricks.com/dev-tools/api/latest/permissions.html#operation/update-all-job-permissions>`_ to make the deployment process consistent.

This means that your job configuration can look like this:

.. code-block:: javascript

    {
        "<environment-name>": [
                {
                    "name": "some-job-with-permissions",
                    // this section shall have a permissions key
                    "permissions":
                        // here goes payload compliant with Permissions API
                        {
                            "access_control_list":
                                [
                                    {
                                        "user_name": "some_user@example.com",
                                        "permission_level": "IS_OWNER",
                                    },
                                    {
                                        "group_name": "some-user-group",
                                        "permission_level": "CAN_VIEW"
                                    }
                                ]
                        }
                }
            ]
    }