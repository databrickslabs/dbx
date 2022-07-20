Fixed properties propagation from cluster policies
==================================================

When working with cluster policies, sometimes it's quirky to specify all the fixed fields in the deployment configuration.

To make this simpler, a new functionality is introduced - this functionality is called "property propagation".

How this works?
---------------

* Specify :code:`policy_name` property in the :code:`new_cluster` section
* :code:`dbx` will automatically fetch the policy definition using `Cluster Policies API <https://docs.databricks.com/dev-tools/api/latest/policies.html>`_
* Job cluster definition will be updated accordingly with all fixed properties provided from the policy definition during the :code:`dbx deploy` step
* There is no need to specify :code:`policy_id` manually, it will be automatically added to the cluster definition

Caveats
-------

* :code:`policy_name` shall exist in your Cluster Policies
* If there are any conflicting configurations provided (i.e. deployment configuration has value provided, and this value is not the same as in the policy) - deployment will fail
* Job properties adjustment happens **only** during the :code:`dbx deploy` step, and it's applied only when there is a :code:`new_cluster` with :code:`policy_name` property specified
* Only the `fixed properties <https://docs.databricks.com/administration-guide/clusters/policies.html#fixed-policy>`_ will be automatically propagated
