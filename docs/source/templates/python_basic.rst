Basic Python Template
=====================

.. contents::
   :depth: 1
   :local:

To create a project from this template, please run the following command:

.. code-block::

    dbx init --template=python_basic

The new project will be located in a folder with the chosen project name.

Project file structure
----------------------

Your generated template will have some generic parts, and some will be CI tool specific.

The clean project structure, without any CI-related files will look like this:

.. code-block:: bash

    .
    ├── .dbx
    │   ├── lock.json  # please note that this file shall be ignored and not added to your git repository.
    │   └── project.json
    ├── .gitignore
    ├── README.md
    ├── conf
    │   ├── deployment.yml
    │   └── test
    │       └── sample.yml
    ├── pytest.ini
    ├── sample_project
    │   ├── __init__.py # <- this is the root folder of your Python package
    │   ├── common.py # <- this file contains a generic class called Job, which provides you all necessary tools, such as Spark and DBUtils
    │   └── jobs
    │       ├── __init__.py
    │       └── sample
    │           ├── __init__.py
    │           └── entrypoint.py
    ├── setup.py
    ├── tests
    │   ├── integration
    │   │   └── sample_test.py
    │   └── unit
    │       └── sample_test.py

Here are some comments about this structure:

* :code:`.dbx` folder is an auxiliary folder, where metadata about environments and execution context is located.
* :code:`sample_project` - Python package with your code (the directory name will follow your project name)
* :code:`tests` - directory with your package tests
* :code:`conf/deployment.yml` - deployment configuration file. Please note that this file is used to configure the *job* deployment properties, such as dependent libraries, tasks, cluster sizes etc.

Please note that this project mostly follows the classical Python package structure as described `here <https://docs.python-guide.org/writing/structure/>`_.

The :code:`conf/deployment.yml` is one of the main components that allows you to flexibly describe your jobs definitions.

All CI tools in this template are following the same concepts, described in `this section <../generic_devops.html>`_.

Depending on your choices of the CI tool during the project creation, your project structure will look like this:

.. tabs::

    .. tab:: GitHub Actions

        .. code-block::

            <file structure as above>
            ├── .github
            │   └── workflows
            │       ├── onpush.yml
            │       └── onrelease.yml

       Some explanations regarding structure:

        * :code:`.github/workflows/` - workflow definitions for GitHub Actions, in particular:

            * :code:`.github/workflows/onpush.yml` defines the CI pipeline logic
            * :code:`.github/workflows/onrelease.yml` defines the CD pipeline logic

    .. tab:: Azure DevOps

        .. code-block::

            <file structure as above>
            ├── azure-pipelines.yml

       Some explanations regarding structure:
        - :code:`azure-pipelines.yml` - Azure DevOps Pipelines workflow definition with both CI and CD pipeline logic.

    .. tab:: GitLab

        .. code-block::

            <file structure as above>
            ├── .gitlab-ci.yml

       Some explanations regarding structure:
        - :code:`.gitlab-ci.yml` - GitLab CI/CD workflow definition with both CI and CD pipeline logic.

After generating the project, we'll need to setup the local development environment.

Local development environment
-----------------------------

* Create new conda environment and activate it:

.. code-block::

    conda create -n <your-environment-name> python=3.7.5
    conda activate <your-environment-name>

* If you would like to be able to run local unit tests, you'll need JDK. If you don't have one, it can be installed via:

.. code-block::

    conda install -c anaconda "openjdk=8.0.152"

* Move the shell to the project directory:

.. code-block::

    cd <project_name>

* Install package in development mode, so your IDE can provide you all required introspection:

.. code-block::

    pip install -e ".[dev]"

At this stage, you have the following:

* Configured Python package
* Configured environment for local development

Running local tests and writing code
------------------------------------

Now, you can open the project in your IDE. Don't forget to point the IDE to the given conda environment name for a full code introspection.

Take a look at the code sample in the :code:`<project_name>/jobs/sample/entrypoint.py`.
This entrypoint file contains an example of an implemented job, based on the abstract :code:`Job` name.
You can see that a configuration object, named :code:`self.conf` referenced in this job - these parameters will be provided from a :code:`conf/test/sample.yml` file during Databricks run.
In the local test, you can override this configuration - please find examples in :code:`tests/unit/sample_test.py`.

To launch local test, simply use the :code:`pytest` framework from the root directory of the project:

.. code-block::

    pytest tests/unit --cov <project_name>

At this stage, you have the following:

* Configured Python package
* Configured environment for local development
* Python package is tested locally

Now, it's time to launch our code on the Databricks clusters.

Running your code on the Databricks clusters
--------------------------------------------

To upload your code from the local environment to Databricks and execute it, there are multiple options:

* execute your code on an interactive (also called all-purpose) cluster
* launch your code as a job on automated cluster
* launch your code as a job on interactive cluster

The third option in general is a **bad idea**, for a very simple reason - your local package will be installed on a cluster-wide level, which means that:

* other users won't be able to override your code, unless your restart the interactive cluster
* you won't be able to install another version of the same library, unless your restart the interactive cluster

Therefore, we're considering two first options.

**Option #1** (execution on interactive cluster) is really suitable when you would like to run your code on interactive cluster during development process to verify that code work properly within real environment.
Your library will be installed in a separate context, which means that other users won't be affected, and you still will be able to install newer versions.

Use this command to execute a specific job on interactive cluster:

.. code-block::

    dbx execute --job=<job-name> --cluster-name=<cluster-name>

Now, if you would like to launch your job on an automated cluster, you probably would like to configure some specific cluster properties, such as size, environment etc.
To do this, please take a look at the :code:`conf/deployment.yml` file. In general, this file follows the Databricks API structures, but it has some additional features, described through this documentation.

After setting the configuration in deployment file, it's time to launch the job. However, we probably don't really want to affect the real job object in our environment.
Instead of this, we're going to perform something called jobless deployment, by providing the :code:`--files-only` property. Please take a look at `this section for more details <../run_submit.html>`_:

.. code-block::

    dbx deploy --jobs=<job-name> --files-only

Now the job can be launched in a run submit mode:

.. code-block::

    dbx launch --as-run-submit --job=<job-name>

At this stage, you have the following:

* Configured Python package
* Configured environment for local development
* Python package is tested locally
* Job has been launched on interactive cluster
* Job has been deployed and launched in a jobless (also called ephemeral or run-submit) mode

Setting up the CI tool
----------------------


Depending on your CI tool, please choose the instruction accordingly:

.. tabs::

    .. tab:: GitHub Actions

        Please do the following:

            * Create a new repository on GitHub
            * Configure :code:`DATABRICKS_HOST` and :code:`DATABRICKS_TOKEN` secrets for your project in `GitHub UI <https://docs.github.com/en/free-pro-team@latest/actions/reference/encrypted-secrets>`_
            * Add a remote origin to the local repo
            * Push the code
            * Open the GitHub Actions for your project to verify the state of the deployment pipeline

        .. warning::

            There is no need to manually create the releases via UI in case of the release.
            Release pipeline will create the release automatically.

    .. tab:: Azure DevOps

        Please do the following:

            * Create a new repository on GitHub or in Azure DevOps (or in any Azure DevOps-compatible git system)
            * Configure :code:`DATABRICKS_HOST` and :code:`DATABRICKS_TOKEN` secrets for your project in `Azure DevOps <https://docs.microsoft.com/en-us/azure/devops/pipelines/release/azure-key-vault?view=azure-devops>`_. Note that secret variables must be mapped to env as mentioned in the `official documentation <https://docs.microsoft.com/en-us/azure/devops/pipelines/process/variables?view=azure-devops&tabs=yaml%2Cbatch#secret-variables>`_.
            * Push the code
            * Open the Azure DevOps UI to check the deployment status

    .. tab:: GitLab

        Please do the following:
            * Create a new repository on Gitlab
            * Configure :code:`DATABRICKS_HOST` and :code:`DATABRICKS_TOKEN` secrets for your project in `GitLab UI <https://docs.gitlab.com/ee/ci/variables/#create-a-custom-variable-in-the-ui>`_
            * Add a remote origin to the local repo
            * Push the code
            * Open the GitLab CI/CD UI to check the deployment status

Please note that to create a release and deploy the job in a normal mode, tag the latest commit in the main branch and push the tags:

.. code-block::

    git fetch
    git checkout main
    git pull
    git tag -a v0.0.1 -m "Release for version 0.0.1"
    git push --tags
