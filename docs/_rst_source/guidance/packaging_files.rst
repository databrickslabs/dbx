Packaging arbitrary files in Python Packages
============================================

Whilst writing a Python Package with :code:`dbx`, you might be in a need to add some arbitrary files to your Python package.

Such arbitrary files can include:

* :code:`*.sql` files where your Spark SQL logic resides
* small static data files that are used in your pipelines
* test files, e.g. when you need to have a file in a specific format

Standard Python packaging tools allow to simply collect, combine and package such arbitrary files together with the main package code.

.. note::

    This example is written for :code:`setup.py`-based packaging.
    For tools like :code:`poetry` and another packaging formats please check their respective docs.


Referencing files
-----------------

First of all, we'll need to reference files in the :code:`setup.py` files.

Imagine having the following project structure:

.. code-block::

    .
    ├── <package-name>
    │       ├── __init__.py
    │       └── resources
    │           ├── raw
    │           │   └── username.csv
    │           └── sql
    │               └── create_table.sql
    ├── setup.py

It's a good practice to keep all arbitrary files in a separate directory (in this case it's located in :code:`<package-name>/resources`.

In the :code:`setup.py` the :code:`package_data` field is responsible for referencing files from this folder:


.. code-block::
    :emphasize-lines: 4

    from setuptools import setup
    setup(
        ...
        package_data={'': ['resources/sql/*.sql', "resources/raw/*.csv"]},
        ...
    )

Using the referenced files
--------------------------


To access the referenced files, do the following in Python:


.. code-block::

    import pkg_resources

    raw_csv_path = pkg_resources.resource_filename(
        "<package-name>", "resources/raw/username.csv"
    )
    query_path = pkg_resources.resource_filename(
        "<package-name>", "resources/sql/create_table.sql"
    )

The provided paths can be used to locally read these files for any purpose.

