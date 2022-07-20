Handling jobless deployments with Run Submit API
================================================

Sometimes during development lifecycle, you don't want to update or even create a job definition out of your project files.

Some examples:

* You're going to use solely Run Submit API
* You want to update or change job definitions only when you release the job
* Multiple users working in parallel on the same job

In such use-cases, `Run Submit API <https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-submit>`_ is much a simpler way to work with the deployments.

Deploying with --files-only
---------------------------

To deploy files without reflecting the deployment definition to jobs, do the following:

.. code-block::

    dbx deploy --files-only

Launching with Run Submit API
-----------------------------

To launch latest file-based deployment, do the following:

.. code-block::

    dbx launch --as-run-submit --job=<job-name>