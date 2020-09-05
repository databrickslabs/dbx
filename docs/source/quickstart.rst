.. _quickstart:

Quickstart
==========

Prerequisites
-------------

When using :code:`dbx`, you need Python 3.X and :code:`pip` or :code:`conda` for package management.

Installing dbx
--------------

Download the .whl file which comes together with this documentation and install :code:`dbx` via :code:`pip`:

.. code-block:: python

    pip install /path/to/dbx.whl

.. note::

    :code:`dbx` is developed on MacOS and tested on Linux with Python 3.+. If you run into a problem running :code:`dbx` on Windows, please raise an issue on GitHub.

Starting from a template
------------------------
If you already have an existing project, you can skip this step and move directly to the next one.

As a first step, you need to create a project from a template. You can use your own template, or you can choose from existing templates:

.. code-block:: python

    cookiecutter --no-input \
        https://github.com/databrickslabs/cicd-templates.git \
        project_name="sample"

Configuring environments
------------------------

Move the shell into the project directory and configure :code:`dbx`.

.. note::

    :code:`dbx` heavily relies on `databricks-cli <https://docs.databricks.com/dev-tools/cli/index.html>`_ and uses the same set of profiles.
    Please configure your profiles in advance using :code:`databricks configure` command as described `here <https://docs.databricks.com/dev-tools/cli/index.html#set-up-authentication>`_.

Create a new environment via given command:

.. code-block:: python

    dbx configure \
        --name="test" \
        --profile="test" \
        --workspace-dir="/dbx/projects/sample"

This command will configure environment by given profile and store project in a given :code:`workspace-dir` as an MLflow experiment.

Interactive execution
---------------------

.. note::

    :code:`dbx` expects that cluster for interactive execution supports :code:`%pip` and :code:`%conda` magic `commands <https://docs.databricks.com/libraries/notebooks-python-libraries.html>`_ in case if you use additional options (requirements, package and conda-environment).

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

Next step would be to configure your deployment objects. To make this process easy and flexible, we're using JSON for configuration.

.. note::

    As you can notice, a lot of elements in the deployment file reference other paths.
    For big deployments, we recommend to generate the deployment file programmatically, for example via `Jsonnet <https://jsonnet.org>`_.


By default, deployment configuration is stored in :code:`.dbx/deployment.json`.
The main idea of  is to provide a flexible way to configure job with it's dependencies.

.. literalinclude:: ../../dbx/template/deployment.json
    :caption: .dbx/deployment.json

A couple of examples for deployment definition could be found here:

.. literalinclude:: ../../tests/templates/deployment-aws.json
    :caption: AWS example - deployment.json

.. literalinclude:: ../../tests/templates/deployment-azure.json
    :caption: Azure example - deployment.json

Deployment
----------

After you've configured the `deployment.json` file, it's time to perform an actual deployment:

.. code-block:: python

    dbx deploy \
        --environment=test \
        --requirements=<requirements.txt>

You can optionally provide requirements.txt file, all requirements will be automatically added to the job definition.

Launch
------

Finally, after deploying all your job-related files, you launch the job via the following command:

.. code-block:: python

    dbx launch --environment=test --job=sample

