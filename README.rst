dbx by Databricks Labs
======================

.. image:: https://raw.githubusercontent.com/databrickslabs/dbx/master/images/logo.svg
    :width: 200
    :height: 200
    :alt: logo
    :align: center

DataBricks CLI eXtensions - aka :code:`dbx` is a CLI tool for advanced Databricks jobs management.

|docs| |pypi| |build| |codecov| |lgtm-alerts| |lgtm-code-quality| |downloads| |black|

.. |docs| image:: https://img.shields.io/readthedocs/dbx?style=for-the-badge
    :target: https://dbx.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. |pypi| image:: https://img.shields.io/pypi/v/dbx?color=green&style=for-the-badge
    :target: https://pypi.org/project/dbx/
    :alt: Latest Python Release

.. |build| image:: https://img.shields.io/github/workflow/status/databrickslabs/dbx/build/main?style=for-the-badge
    :alt: GitHub Workflow Status (branch)
    :target: https://github.com/databrickslabs/dbx/actions/workflows/onpush.yml

.. |codecov| image:: https://img.shields.io/codecov/c/github/databrickslabs/dbx?style=for-the-badge&token=S7ADH3W2E3
    :target: https://codecov.io/gh/databrickslabs/dbx

.. |lgtm-alerts| image:: https://img.shields.io/lgtm/alerts/github/databrickslabs/dbx?style=for-the-badge
    :target: https://lgtm.com/projects/g/databrickslabs/dbx/alerts

.. |lgtm-code-quality| image:: https://img.shields.io/lgtm/grade/python/github/databrickslabs/dbx?style=for-the-badge
    :target: https://lgtm.com/projects/g/databrickslabs/dbx/context:python

.. |downloads| image:: https://img.shields.io/pypi/dm/dbx?style=for-the-badge

.. |black| image:: https://img.shields.io/badge/code%20style-black-000000.svg?style=for-the-badge
    :target: https://github.com/psf/black
    :alt: We use black for formatting

.. contents:: :local:

Concept
-------

:code:`dbx` simplifies jobs launch and deployment process across multiple environments.
It also helps to package your project and deliver it to your Databricks environment in a versioned fashion.
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

Quickstart
----------

Please refer to the `Quickstart section <https://dbx.readthedocs.io/en/latest/quickstart.html>`_.

Documentation
-------------

Please refer to the `docs page <https://dbx.readthedocs.io/en/latest/index.html>`_.

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
| `Databricks Stack CLI <https://docs.databricks.com/dev-tools/cli/stack-cli.html>`_                 | Databricks Stack CLI is a great component for managing a stack of objects. dbx concentrates on the versioning and packaging jobs together, not treating files and notebooks as a separate component.                                                                               |
+----------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Limitations
-----------

* Development:

    * | :code:`dbx` currently doesn't provide interactive debugging capabilities.
      | If you want to use interactive debugging, you can use `Databricks Connect <https://docs.databricks.com/dev-tools/databricks-connect.html>`_ + :code:`dbx` for deployment operations.
    * :code:`dbx execute` only supports Python-based projects which use local files (Notebooks or Repos are not supported in :code:`dbx execute`).
    * :code:`dbx execute` can only be used on clusters with Databricks ML Runtime 7.X or higher.

* General:

    * :code:`dbx` doesn't support `Delta Live Tables <https://databricks.com/product/delta-live-tables>`_ at the moment.
    * | :code:`host` in your profile configuration in :code:`~/.databrickscfg` shall only consist of two parts: :code:`{scheme}://netlog`, e.g. :code:`https://some-host.cloud.databricks.com`.
      | Strings like :code:`https://some-host.cloud.databricks.com/?o=XXXX#` are not supported. As a symptom if this you might the the error below:

.. code-block::

    raise MlflowException("%s. Response body: '%s'" % (base_msg, response.text))
    mlflow.exceptions.MlflowException: API request to endpoint was successful but the response body was not in a valid JSON format.

Versioning
----------

For CLI interfaces, we support `SemVer <https://semver.org/>`_ approach. However, for API components we don't use SemVer as of now.
This may lead to instability when using :code:`dbx` API methods directly.

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

Please find more details about contributing to :code:`dbx` in the contributing `doc <https://github.com/databrickslabs/dbx/blob/master/contrib/CONTRIBUTING.md>`_.






