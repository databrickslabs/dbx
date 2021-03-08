Integration with orchestration tools
====================================

Development of data pipelines might be a challenging task not only from the CI perspective, but from the orchestration side as well.
In this section we describe two most common orchestration tools - `Azure Data Factory <https://azure.microsoft.com/en-us/services/data-factory/>`_ and `Airflow <https://airflow.apache.org/>`_.

Before describing details of integration with these tools, let's take a look on the logical approach and separate responsibility between CI pipeline and orchestration pipeline.

CI pipelines and orchestration pipelines
----------------------------------------

:code:`dbx` is responsible for helping end user with *CI pipelines* and *local development*. By this, we mean the following tasks:

* deploying new jobs in a consistent, versioned fashion
* deploying jobs across different environments
* delivering dependent files, such as whl packages and .json configurations in a versioned fashion to DBFS

:code:`dbx` can launch jobs, but it's not the best tool for chaining jobs or launching them in a ordered fashion.

In contrast, aforementioned solutions are oriented to *orchestrate* pipelines, for example:

* Create a set of pipeline steps
*