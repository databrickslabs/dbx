Named properties support
========================

Since version 0.2.2 you can also use :code:`dbx` for name-based properties instead of providing ids.


The following properties are supported:

* :code:`existing_cluster_name` will be automatically replaced with :code:`existing_cluster_id`
* :code:`new_cluster.instance_pool_name` will be automatically replaced with :code:`new_cluster.instance_pool_id`
* :code:`new_cluster.driver_instance_pool_name` will be automatically replaced with :code:`new_cluster.driver_instance_pool_id`
* :code:`new_cluster.aws_attributes.instance_profile_name` will be automatically replaced with :code:`new_cluster.aws_attributes.instance_profile_arn`

By this simplification, you don't need to look-up for these id-based properties, you can simply provide the names.

Here are some examples in JSON and YAML:

.. tabs::

   .. tab:: JSON

      .. literalinclude:: ../../tests/deployment-configs/05-json-with-named-properties.json
         :language: JSON

   .. tab:: YAML

      .. literalinclude:: ../../tests/deployment-configs/05-yaml-with-named-properties.yaml
         :language: YAML


.. note::

    Named properties are also supported for Jobs API 2.1 - simply provide them on the :code:`new_cluster` level.

