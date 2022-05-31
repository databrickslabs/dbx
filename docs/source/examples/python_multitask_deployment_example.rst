Python Multi Task Deployment YAML Example
=========================================

Project structure
-----------------

.. code-block:: bash

  .
  ├── .dbx
  │   ├── lock.json  # please note that this file shall be ignored and not added to your git repository.
  │   └── project.json
  ├── .gitignore
  ├── README.md
  ├── conf
  │   ├── deployment.yml
  │   └── test
  │       └── sample.yml
  ├── pytest.ini
  ├── sample_project
  │   ├── __init__.py # <- this is the root folder of your Python package
  │   ├── common.py # <- this file contains a generic class called Job, which provides you all necessary tools, such as Spark and DBUtils
  │   └── jobs
  │       ├── __init__.py
  │       ├── your-file-01.py
  │       └── your-file-02.py
  ├── setup.py
  ├── tests
  │   ├── integration
  │   │   └── sample_test.py
  │   └── unit
  │       └── sample_test.py
  └── requirements.txt


Useful commands
---------------

* Get list of DBR versions - :code:`$ databricks clusters spark-versions`
* Get list of node_type_ids - :code:`$ databricks clusters list-node-types`


Example deployment file
-----------------------

.. note::

    Documentation tries to list all the options, based on your need some options may not be relevant.
    For example, if instance_pool_name is used, then note_type_id may not be relevant and so on.

.. literalinclude:: ./python_mtj.yml
    :language: yaml



