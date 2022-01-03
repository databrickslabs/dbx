Basic Python Template
=====================

To create a project from this template, please run the following command:

.. code-block::

    dbx init --template=python_basic

Project file structure
----------------------

Your generated template will gave some generic parts, and some of them will be CI tool specific.
The clean project structure, without any CI-related files will look like this:

.. code-block::

    .
    ├── .dbx
    │   ├── lock.json
    │   └── project.json
    ├── .gitignore
    ├── README.md
    ├── conf
    │   ├── deployment.yml
    │   └── test
    │       └── sample.yml
    ├── pytest.ini
    ├── sample_project
    │   ├── __init__.py
    │   ├── common.py
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
    └── unit-requirements.txt

Depending on your choices of the CI tool during the project creation, your project structure will look like this:

.. tabs::

    .. tab:: GitHub Actions

        .. code-block::

            .
            ├── .dbx
            │   ├── lock.json
            │   └── project.json
            ├── .github
            │   └── workflows
            │       ├── onpush.yml
            │       └── onrelease.yml
            ├── .gitignore
            ├── README.md
            ├── conf
            │   ├── deployment.yaml
            │   └── test
            │       └── sample.yaml
            ├── pytest.ini
            ├── sample_project
            │   ├── __init__.py
            │   ├── common.py
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
            └── unit-requirements.txt

       Some explanations regarding structure:

        * :code:`.dbx` folder is an auxiliary folder, where metadata about environments and execution context is located.
        * :code:`sample_project` - Python package with your code (the directory name will follow your project name)
        * :code:`tests` - directory with your package tests
        * :code:`conf/deployment.yaml` - deployment configuration file. Please note that this file is used to configure the *job* deployment properties, such as dependent libraries, tasks, cluster sizes etc.
        * :code:`.github/workflows/` - workflow definitions for GitHub Actions, in particular:

            * :code:`.github/workflows/onpush.yml` defines the CI pipeline logic
            * :code:`.github/workflows/onrelease.yml` defines the CD pipeline logic

        You can read more about CI and CD pipelines purposes here.
