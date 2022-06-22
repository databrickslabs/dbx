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

:code:`dbx` comes with a set of pre-defined `templates <templates_pointer.html>`_ and a command to use them straight away.
However, if you don't like the project structure defined in the provided templates, feel free to use the instruction below for full customization.


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

Preparing Deployment Config
---------------------------

Next step would be to configure your deployment objects. The main idea of the deployment file is to provide a flexible way to configure jobs with their dependencies.
To make this process easy and flexible, we support two options to define the configuration.

#. JSON: :code:`conf/deployment.json`
#. YAML: :code:`conf/deployment.(yml|yaml)`

If the above options are located relative to the project root directory they will be auto-discovered, else you will need to explicitly specify the file to :code:`dbx deploy` using the option :code:`--deployment-file=./path/to/file.(json|yml|yaml)`.
The :code:`--deployment-file` option also allows you to use multiple different deployment files.

.. note::

    Within the deployment config, if you find that you have duplicated parts like cluster definitions or retry config or permissions or anything else,
    and you are finding it hard to manage the duplications, we recommend you either use `YAML <http://yaml.org/spec/1.2/spec.html>`_ or `Jsonnet <https://jsonnet.org>`_.

    Yaml is supported by dbx where as with Jsonnet, you are responsible for generating the json file through Jsonnet compilation process.

.. note::

    :code:`dbx` supports passing environment variables into both JSON and YAML based deployment files. Please read more about this functionality :doc:`here <environment_variables>`.

.. note::

    Since version 0.4.1 :code:`dbx` :doc:`supports Jinja2 <jinja2_support>` rendering for JSON and YAML based configurations.


JSON
****

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


YAML
****

You can define re-usable definitions in yaml. Here is an example yaml and its json equivalent:

.. note::
    The YAML file needs to have a top level :code:`environments` key under which all environments will be listed.
    The rest of the definition is the same as it is for config using json. It follows the
    `Databricks Jobs API <https://docs.databricks.com/dev-tools/api/latest/jobs.html>`_ with the same auto
    versioning and upload of local files referenced with in the config.

.. tabs::

    .. tab:: YAML

        .. literalinclude:: ../../tests/deployment-configs/03-multitask-job.yaml
            :language: YAML

    .. tab:: JSON Equivalent

        .. literalinclude:: ../../tests/deployment-configs/03-multitask-job.json
            :language: JSON


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
