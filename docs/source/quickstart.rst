.. _quickstart:

Quickstart
==========

Prerequisites
-------------

When using :code:`dbx`, you need Python 3.X and :code:`pip` or :code:`conda` for package management.

Installing dbx
--------------

You install :code:`dbx` via :code:`pip`:

.. code-block:: python

    pip install dbx

Alternatively, you can install :code:`dbx` via :code:`conda`:

.. code-block:: python

    conda install dbx

.. note::

    :code:`dbx` is developed on MacOS and tested on Linux with Python 3.+. If you run into a problem running :code:`dbx` on Windows, please raise an issue on GitHub.

Starting from a template
------------------------

As a first step, you need to create a project from a template. You can use your own template, or you can choose from existing templates:

.. code-block:: python

    cookiecutter --no-input \
        https://github.com/databrickslabs/cicd-templates.git \
        project_name="sample"

    cd sample

Initializing dbx in the project directory
-----------------------------------------

After creating a project, initialize :code:`dbx` inside a directory. Provide any project name as a parameter:

.. code-block:: python

    dbx init --project-name="sample"

Configuring environments
------------------------

.. note::

    :code:`dbx` heavily relies on `databricks-cli <https://docs.databricks.com/dev-tools/cli/index.html>`_ and uses the same set of profiles.
    Please configure your profiles in advance using :code:`databricks configure` command as described `here <https://docs.databricks.com/dev-tools/cli/index.html#set-up-authentication>`_.

Create a new environment via given command:

.. code-block:: python

    dbx configure \
        --name="test" \
        --profile="some-profile-name" \
        --workspace-dir="/dbx/projects/sample"

This command will configure environment by given profile and store project in a given :code:`workspace-dir` as an MLflow experiment.

Interactive execution
---------------------

To execute the code in an interactive fashion, we recommend to use interactive clusters.
This command executes content from a given source file on a provided cluster.
You need to provide either :code:`cluster-id` or :code:`cluster-name`, and a :code:`--source-file` parameter.

.. code-block:: python

    dbx execute \
        --cluster-id="<some-cluster-id>" \
        --source-file="some/entrypoint.py" \
        --package="dist/my-package.whl" \
        --requirements="requirements.txt" \
        --conda-environment="conda-env.yml"

You can also provide parameters to install .whl packages before launching code from the source file, as well as installing dependencies from pip-formatted requirements file or conda environment yml config.

Preparing deployment file
-------------------------

Next step would be to configure your deployment objects. To make this process easy and flexible, we're using `Jsonnet <https://jsonnet.org/>`_ .
By default, deployment configuration is stored in :code:`.dbx/deployment.jsonnet`.
The main idea of  is to provide a flexible way to configure job with a lot of dependencies.

.. literalinclude:: ../../dbx/template/deployment.jsonnet
    :caption: .dbx/deployment.jsonnet

Deployment
----------

After you've configured the `deployment.jsonnet` file, it's time to perform an actual deployment:

.. code-block:: python

    dbx deploy --environment=test

Launch
------

Finally, after deploying all your job-related files, you launch the job via the following command:

.. code-block:: python

    dbx launch --environment=test --job=sample

