# :material-package-variant-plus: Packaging arbitrary files in Python Packages

Whilst writing a Python Package with `dbx`, you might require to add some arbitrary files to your Python package.

Such arbitrary files can include:

-   `*.sql` files where your Spark SQL logic resides
-   small static data files that are used in your pipelines
-   test files, e.g. when you need to have a file in a specific format

Standard Python packaging tools allow to simply collect, combine and
package such arbitrary files together with the main package code.

!!! note

    This example is written for `setup.py`-based packaging. For tools like
    `poetry` and another packaging formats please check their respective docs.

## :material-link-plus: Referencing files

First of all, we'll need to reference files in the `setup.py`.

Imagine having the following project structure:

```
.
├── <package-name>
│       ├── __init__.py
│       └── resources
│           ├── raw
│           │   └── username.csv
│           └── sql
│               └── create_table.sql
├── setup.py
```

In the `setup.py` the `package_data` field is responsible for
referencing files from this folder:

```python
from setuptools import setup
setup(
    ...
    package_data={'': ['resources/sql/*.sql', "resources/raw/*.csv"]},
    ...
)
```
!!! tip

    It's a good practice to keep all arbitrary files in a separate directory.<br/>
    In this example it's located in `<package-name>/resources`.

## :material-hook: Using the referenced files

To access the referenced files, do the following in Python:

```python
import pkg_resources

raw_csv_path = pkg_resources.resource_filename(
    "<package-name>", "resources/raw/username.csv"
)
query_path = pkg_resources.resource_filename(
    "<package-name>", "resources/sql/create_table.sql"
)
```

The provided paths can be used to read these files for any purpose.
