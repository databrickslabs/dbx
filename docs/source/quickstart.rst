.. _quickstart:

Quickstart
==========

Installing dbx
--------------

You install :code:`dbx` via :code:`pip`:

.. code-block:: python

    pip install dbx

Alternatively, you can install :code:`dbx` via :code:`conda`:

.. code-block:: python

    conda install dbx

.. note::

    :code:`dbx` is developed on MacOS and tested on Linux with Python 3.+. If you run into issues running :code:`dbx` on Windows, please raise an issue in GitHub.

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

Create a new environment via given command:

.. code-block:: python

    dbx configure \
        --name="test" \
        --profile="some-profile-name" \
        --workspace-dir="/dbx/projects/sample"

This will configure a storage for project, and MLflow storage for deployment tracking.

Preparing deployment file
-------------------------

Next step would be to configure your deployment objects. To make this process easy and flexible, we're using jsonnet format.
The main idea of `.dbx/deployment.jsonnet` is to provide a flexible way to configure job with a lot of dependencies.

.. include:: ../../dbx/template/deployment.jsonnet
   :code: json
   :literal:

Deployment via dbx
------------------

After you've configured the `deployment.jsonnet` file, it's time to perform an actual deployment:

.. code-block:: python

    dbx deploy --environment=test

Launch via dbx
--------------

Finally, after deploying all your job-related files, you launch the job via the following command:

.. code-block:: python

    dbx launch --environment=test --job=sample

