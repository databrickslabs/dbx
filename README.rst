dbx by Databricks Labs
======================

DataBricks eXtensions - aka :code:`dbx` is a CLI tool for advanced Databricks jobs management.

.. image:: https://github.com/databrickslabs/dbx/actions/workflows/onpush.yml/badge.svg?branch=master
    :target: https://github.com/databrickslabs/dbx/actions/workflows/onpush.yml

.. image:: https://codecov.io/gh/databrickslabs/dbx/branch/master/graph/badge.svg?token=S7ADH3W2E3
    :target: https://codecov.io/gh/databrickslabs/dbx

.. image:: https://img.shields.io/lgtm/alerts/g/databrickslabs/dbx.svg?logo=lgtm&logoWidth=18
    :target: https://lgtm.com/projects/g/databrickslabs/dbx/alerts

.. image:: https://img.shields.io/lgtm/grade/python/g/databrickslabs/dbx.svg?logo=lgtm&logoWidth=18
    :target: https://lgtm.com/projects/g/databrickslabs/dbx/context:python

.. contents:: :local:

Concept
-------

:code:`dbx` simplifies jobs launch and deployment process across multiple environments.
Designed in a CLI-first manner, it is built to be actively used both inside CI/CD pipelines and as a part of local tooling for fast prototyping.

Requirements
------------

* Python Version > 3.6
* :code:`pip` or :code:`conda`

Installation
------------

* with :code:`pip`:

.. code-block::

    pip install dbx

* with :code:`conda`:

.. code-block::

    conda install dbx

Quickstart
----------

    As a prerequisite, you need to install `databricks-cli <https://github.com/databricks/databricks-cli>`_ with a `configured profile <https://docs.databricks.com/dev-tools/cli/index.html#set-up-authentication>`_.
    In this instruction we're based on `Databricks Runtime 7.3 LTS ML <https://docs.databricks.com/release-notes/runtime/7.3ml.html>`_.
    If you don't need to use ML libraries, we still recommend to use ML-based version due to :code:`%pip` magic `support <https://docs.databricks.com/libraries/notebooks-python-libraries.html>`_.


For Python-based deployments, we recommend to use `cicd-templates <https://github.com/databrickslabs/cicd-templates>`_ for quickstart.
However, if you don't like the project structure defined in cicd-templates, feel free to use the instruction below for full customization.

After configuring the profile, please do the following in the root of your project:

- Configure your project environments and storage locations:

.. code-block::

    dbx configure

- Create a :code:`conf/deployment.json` file with specs defined in documentation
- Run :code:`dbx deploy` to perform an initial deployment
- Run :code:`dbx launch --job=<job-name> --trace` to launch the job and trace it's status



Documentation
-------------

Please refer to the docs provided in the `docs <docs>`_ folder.

Differences from other tools
----------------------------

+----------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Tool                                                                                               | Comment                                                                                                                                                                                                                                                                            |
+====================================================================================================+====================================================================================================================================================================================================================================================================================+
| `databricks-cli <https://github.com/databricks/databricks-cli>`_                                   | dbx is NOT a replacement for databricks-cli. Quite the opposite - dbx is heavily dependent on databricks-cli and uses most of the APIs exactly from databricks-cli SDK.                                                                                                            |
+----------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| `mlflow cli <https://www.mlflow.org/docs/latest/cli.html>`_                                        | dbx is NOT a replacement for mlflow cli. dbx uses some of the MLflow APIs under the hood to store serialized job objects, but doesn't use mlflow CLI directly.                                                                                                                     |
+----------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| `Databricks Terraform Provider <https://github.com/databrickslabs/terraform-provider-databricks>`_ | While dbx is primarily oriented on versioned job management, Databricks Terraform Provider provides much wider set of infrastructure settings. In comparison, dbx doesn't provide infrastructure management capabilities, but brings more flexible deployment and launch options.  |
+----------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| `cicd-templates <https://github.com/databrickslabs/cicd-templates>`_                               | cicd-templates is a Python project template, which actively uses dbx for jobs management and CI-related operations. You can choose, whenever you would like to use this template, or use dbx separately and choose the project structure on your own.                              |
+----------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| `Databricks Stack CLI <https://docs.databricks.com/dev-tools/cli/stack-cli.html>`_                 | Databricks Stack CLI is a great component for managing a stack of objects. dbx concentrates on the versioning and packaging jobs together, not treating files and notebooks as a separate component.                                                                               |
+----------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Limitations
-----------

* Python > 3.6
* :code:`dbx execute` can only be used on clusters with Databricks ML Runtime 7.X


Legal Information
-----------------

This software is provided as-is and is not officially supported by Databricks through customer technical support channels.
Support, questions, and feature requests can be communicated through the Issues page of this repo.
Please see the legal agreement and understand that issues with the use of this code will not be answered or investigated by Databricks Support.

Feedback
--------

Issues with :code:`dbx`? Found a bug? Have a great idea for an addition? Feel free to file an `issue <https://github.com/databrickslabs/dbx/issues/new/choose>`_.

Contributing
------------

Please find more details about contributing to :code:`dbx` in the contributing `doc <https://github.com/databrickslabs/dbx/blob/master/CONTRIBUTING.md>`_.






