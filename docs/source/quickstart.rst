.. _quickstart:

Quickstart
==========

Prerequisites
-------------

- Python >=3.6 environment on your local machine
- `databricks-cli`_ with a `configured profile <https://docs.databricks.com/dev-tools/cli/index.html#set-up-authentication>`_


In this instruction we're based on `Databricks Runtime 7.3 LTS ML <https://docs.databricks.com/release-notes/runtime/7.3ml.html>`_.
If you don't need to use ML libraries, we still recommend to use ML-based version due to :code:`%pip` magic `support <https://docs.databricks.com/libraries/notebooks-python-libraries.html>`_.

Installing dbx
--------------

Install :code:`dbx` via :code:`pip`:

.. code-block:: python

    pip install dbx

Starting from a template (Python)
---------------------------------
If you already have an existing project, you can skip this step and move directly to the next one.

For Python-based deployments, we recommend to use `cicd-templates <https://github.com/databrickslabs/cicd-templates>`_ for quickstart.
However, if you don't like the project structure defined in cicd-templates, feel free to use the instruction below for full customization.


Configuring environments
------------------------

Move the shell into the project directory and configure :code:`dbx`.

.. note::

    :code:`dbx` heavily relies on `databricks-cli`_ and uses the same set of profiles.
    Please configure your profiles in advance using :code:`databricks configure` command as described `here <https://docs.databricks.com/dev-tools/cli/index.html#set-up-authentication>`_.

Create a new environment configuration via given command:

.. code-block:: python

    dbx configure \
        --profile=test # name of your profile, omit if you would like to use the DEFAULT one

This command will configure a project file in :code:`.dbx/project.json` file. Feel free to repeat this command multiple times to reconfigure the environment.

Preparing deployment file
-------------------------

Next step would be to configure your deployment objects. To make this process easy and flexible, we're using JSON for configuration.

.. note::

    As you can notice, a lot of elements in the deployment file are referencing other paths.
    For big deployments, we recommend to generate the deployment file programmatically, for example via `Jsonnet <https://jsonnet.org>`_.


By default, deployment configuration is stored in :code:`conf/deployment.json`.
The main idea of the deployment file is to provide a flexible way to configure job with it's dependencies.
You can use multiple different deployment files, providing the filename as an argument to :code:`dbx deploy` via :code:`--deployment-file=/path/to/file` option.
Here are some samples of deployment files for different cloud providers:

.. tabs::

   .. tab:: AWS

      .. literalinclude:: ../../tests/deployment-configs/aws-example.json
         :language: JSON

   .. tab:: Azure

      .. literalinclude:: ../../tests/deployment-configs/azure-example.json
         :language: JSON

   .. tab:: GCP

      .. literalinclude:: ../../tests/deployment-configs/gcp-example.json
         :language: JSON

Expected structure of the deployment file is the following:

.. code-block:: javascript

    {
        // you may have multiple environments defined per one deployment.json file
        "<environment-name>": {
          "jobs": [
                // here goes a list of jobs, every job is one dictionary
                {
                    "name": "this-parameter-is-required!",
                    // everything else is as per Databricks Jobs API
                    // however, you might reference any local file (such as entrypoint or job configuration)
                    "spark_python_task": {
                        "python_file": "path/to/entrypoint.py" // references entrypoint file relatively to the project root directory
                    },
                    "parameters": [
                        "--conf-file",
                        "conf/test/sample.json" // references configuration file relatively to the project root directory
                    ]
                }
            ]
          }
    }

As you can see, we simply follow the `Databricks Jobs API <https://docs.databricks.com/dev-tools/api/latest/jobs.html>`_ with one enhancement -
any local files can be referenced and will be uploaded to dbfs in a versioned way during the :code:`dbx deploy` command.

Interactive execution
---------------------

.. note::

    :code:`dbx` expects that cluster for interactive execution supports :code:`%pip` and :code:`%conda` magic `commands <https://docs.databricks.com/libraries/notebooks-python-libraries.html>`_.


The :code:`dbx execute` executes given job on an interactive cluster.
You need to provide either :code:`cluster-id` or :code:`cluster-name`, and a :code:`--job` parameter.

.. code-block:: python

    dbx execute \
        --cluster-name=some-name \
        --job=your-job-name

You can also provide parameters to install .whl packages before launching code from the source file, as well as installing dependencies from pip-formatted requirements file or conda environment yml config.

Deployment
----------

After you've configured the `deployment.json` file, it's time to perform an actual deployment:

.. code-block:: python

    dbx deploy \
        --environment=test

You can optionally provide requirements.txt file, all requirements will be automatically added to the job definition.
Please refer to the full description of deploy command in the CLI section for more options on setup.

Launch
------

Finally, after deploying all your job-related files, you can launch the job via the following command:

.. code-block:: python

    dbx launch --environment=test --job=sample

Please refer to the full description of launch command in the CLI section for more options.

.. _databricks-cli: https://docs.databricks.com/dev-tools/cli/index.html
