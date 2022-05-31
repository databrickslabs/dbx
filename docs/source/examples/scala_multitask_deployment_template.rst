Scala Multi Task Deployment YAML Example
========================================

Project structure
-----------------

.. code-block:: bash

  .
  ├── .dbx
  │   ├── lock.json  # please note that this file shall be ignored and not added to your git repository.
  │   └── project.json
  ├── .gitignore
  ├── README.md
  ├── build.sbt
  │
  ├── conf
  │   ├── deployment.yml
  │
  ├── project
  │   ├── assembly.sbt
  │   ├── build.properties
  │   └── plugins.sbt
  ├── project
  │   ├── target
  ├── target
  ├── src
  │   ├── main
  │   │   └── scala
  │   └── test
  │       └── scala


Useful commands
---------------

* Get list of DBR versions - :code:`$ databricks clusters spark-versions`
* Get list of node_type_ids - :code:`$ databricks clusters list-node-types`


Example deployment file
-----------------------

.. note::

    Documentation tries to list all the options, based on your need some options may not be relevant.
    For example, if instance_pool_name is used, then note_type_id may not be relevant and so on.

.. literalinclude:: ./scala_mtj.yml
    :language: yaml
