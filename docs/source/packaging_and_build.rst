Core dependency management and build options
============================================

Building the project package
----------------------------


By default, :code:`dbx` is heavily oriented towards Python-based projects that could be compiled into a .whl file.
To simplify this functionality during deployment, dbx will automatically create a .whl file in the :code:`dist/` folder during the :code:`dbx deploy` command.

However, in some cases there is no need to build the Python-based .whl file. Such cases might be:

* You're not using Python in your project (e.g. your project is written in Scala or Java)
* You're only using notebooks
* You're using an external packaging tool such as `poetry <https://python-poetry.org/>`_

To disable the local package rebuild during deployment, please provide :code:`--no-rebuild` switch during deployment:

.. code-block:: console

    dbx deploy --no-rebuild

Core dependency management
--------------------------

.. note::

    By **core dependency package** we mean the .whl file located in the :code:`dist/` folder.

The core dependency package is very important part of the dependencies that needs to b deployed in case if you develop your job with Python packaging mechanisms.

By default, :code:`dbx` uploads the package file and adds it into the job definition (if it's a single task job), or into every task (if it's a multitask-job).

In the resulting definition you'll be able to see the package file referenced as:

.. code-block::

    "libraries": [
            {
                "whl": "dbfs:/Shared/dbx/projects/<some_project>/<some-hash>/artifacts/dist/<some-package>-<version>-py3-none-any.whl"
            }
    ]

However, in some cases you would like to omit the core dependency reference in the job definition, for example:

* You're not using Python in your project (e.g. your project is written in Scala or Java)
* You're only using notebooks and they're not dependent on the .whl file since the code is shipped together with Repos

In such cases, you can do one of the following:

1. | Disable package file reference globally for the whole deployment.
   | In this case package file won't be added to the :code:`libraries` section nor on the job level, neither on the task level.
   | This could be achieved by providing :code:`--no-package` switch to the deployment command:

.. code-block:: console

    dbx deploy --no-package

2. | You can disable package file references on a per-job or per-task level
   | by providing the following in the deployment configuration file:

.. tabs::

   .. tab:: JSON

      .. literalinclude:: ../../tests/deployment-configs/07-json-packaging-example.json
         :language: JSON

   .. tab:: YAML

      .. literalinclude:: ../../tests/deployment-configs/07-yaml-packaging-example.yaml
         :language: YAML

As per examples above - it's possible to provide a per-job or per-task deployment properties in the :code:`deployment_config` section.
