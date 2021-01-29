dbx by Databricks Labs
======================

DataBricks eXtensions - aka :code:`dbx` is a CLI tool for advanced jobs management in CI/CD pipelines.

Concept
-------

:code:`dbx` simplifies jobs launch and deployment process across multiple environments.
Designed as a CLI-tool, it is built to be actively used both inside CI/CD pipelines, and as a local CLI tool for fast prototyping.

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

Docs & Examples
---------------

Both documentation and examples could e found in the `docs <docs>`_ folder.

Differences from other tools
----------------------------

+----------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Tool                                                                                               | Comment                                                                                                                                                                                                                                               |
+====================================================================================================+=======================================================================================================================================================================================================================================================+
| `databricks-cli <https://github.com/databricks/databricks-cli>`_                                   | dbx is NOT a replacement for databricks-cli. Quite the opposite - dbx is heavily dependent on databricks-cli and uses most of the APIs exactly from databricks-cli SDK.                                                                               |
+----------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| `mlflow cli <https://www.mlflow.org/docs/latest/cli.html>`_                                        | dbx is NOT a replacement for mlflow cli. dbx uses some of the MLflow APIs under the hood to store serialized job objects, but doesn't use mlflow CLI directly.                                                                                        |
+----------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| `Databricks Terraform Provider <https://github.com/databrickslabs/terraform-provider-databricks>`_ | While dbx is primarily oriented on versioned job management, Databricks Terraform Provider provides much wider set of infrastructure settings. This leads to lesser flexibility of dbx, but more sophisticated deployment and launch options.         |
+----------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| `cicd-templates <https://github.com/databrickslabs/cicd-templates>`_                               | cicd-templates is a Python project template, which actively uses dbx for jobs management and CI-related operations. You can choose, whenever you would like to use this template, or use dbx separately and choose the project structure on your own. |
+----------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| `Databricks Stack CLI <https://docs.databricks.com/dev-tools/cli/stack-cli.html>`_                 | Databricks Stack CLI is a great component for managing a stack of objects. dbx concentrates on the versioning and packaging jobs together, not treating files and notebooks as a separate component.                                                  |
+----------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

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

Issues with :code:`dbx`? Found a bug? Have a great idea for an addition? Feel free to file an issue.

Contributing
------------

Have a great idea that you want to add? Fork the repo and submit a PR!






