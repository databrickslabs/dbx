# :material-language-python: :material-airplane-takeoff: Python quickstart

In this guide we're going to walkthough a typical setup for development purposes.

In the end of this guide you'll have a prepared local environment, as well as capabilities to:

* Run local unit tests
* Deploy workflows to Databricks
* Launch workflows on the Databricks platform

!!! tip

    Although this example is based on [GitHub](https://github.com/) and [GitHub Actions](https://github.com/features/actions),
    `dbx` can be easily and seamlessly used with any CI provider and any Git provider.
    The built-in Python template also can generate pipelines for Azure DevOps and GitLab.

## :fontawesome-solid-list-check: Prerequisites

- :material-github: GitHub account
- :material-git: Git on your local machine
- Conda and :material-language-python: Python 3.8+ on your local machine
- 🧱 Databricks workspace you can use

## :material-cog-play-outline: Preparing the local environment

In this example we'll call our project: **:fontawesome-solid-wand-magic-sparkles: charming-aurora**.

1. Create a new conda environment:

```bash
conda create -n charming-aurora python=3.9
```

2. Activate this environment:

```bash
conda activate charming-aurora
```

3. Install OpenJDK for local Spark tests:

```bash
conda install -c conda-forge openjdk=11.0.15
```

4. Install `dbx`:

```bash
pip install dbx
```

5. Create a new [Databricks API token](https://docs.databricks.com/dev-tools/api/latest/authentication.html) and
   configure the CLI:

```bash
databricks configure --profile charming-aurora --token
```

6. Verify that profile is working as expected:

```bash
databricks --profile charming-aurora workspace ls /
```
7. Now the preparation is done, let’s generate a project skeleton using [`dbx init`](../../../cli/#dbx-init):
```bash
dbx init -p \
    "cicd_tool=GitHub Actions" \
    -p "cloud=<your-cloud>" \
    -p "project_name=charming-aurora" \
    -p "profile=charming-aurora" \
    --no-input
```
8. Step into the newly generated folder:

```bash
cd charming-aurora
```

9. Install dependencies for local environment:

```bash
pip install -e ".[dev]"
```

10. Run local unit tests:
```bash
pytest tests/unit --cov
```

If everything is set up correctly, in the output you'll see the test logs and the coverage report:

``` title="pytest output"
---------- coverage: platform darwin, python 3.9.12-final-0 ----------
Name                                       Stmts   Miss Branch BrPart  Cover
----------------------------------------------------------------------------
charming_aurora/__init__.py                    1      0      0      0   100%
charming_aurora/tasks/__init__.py              0      0      0      0   100%
charming_aurora/tasks/sample_etl_task.py      16      0      0      0   100%
charming_aurora/tasks/sample_ml_task.py       38      0      0      0   100%
----------------------------------------------------------------------------
TOTAL                                         55      0      0      0   100%
```

Tada! :partying_face: 🎉. Your first project has been created and successfully tested.

Now let's dig deeper into the project structure.

## :material-family-tree: Project structure

This is how the project looks like:
```bash title="generate the project tree"
tree -L 3 -I __pycache__ -a -I .git -I .pytest_cache -I .coverage
```

``` shell title="project tree"
.
├── .dbx #(1)
│   ├── lock.json #(2)
│   └── project.json #(3)
├── .github #(4)
│   └── workflows #(5)
│       ├── onpush.yml #(6)
│       └── onrelease.yml #(7)
├── .gitignore #(8)
├── README.md #(9)
├── charming_aurora #(10)
│   ├── __init__.py #(11)
│   ├── common.py #(12)
│   └── tasks #(13)
│       ├── __init__.py #(14)
│       ├── sample_etl_task.py #(15)
│       └── sample_ml_task.py #(16)
├── conf #(17)
│   ├── deployment.yml #(17)
│   └── test #(18)
│       ├── sample_etl_config.yml #(19)
│       └── sample_ml_config.yml #(20)
├── notebooks #(21)
│   └── sample_notebook.py #(22)
├── pyproject.toml #(23)
├── setup.py #(24)
└── tests #(25)
    └── unit #(26)
        ├── conftest.py #(27)
        └── sample_test.py #(28)
```

1. a
2. b
3. c
4. d
5. f
6. e
7. a
8. a
9. wd
10. h
11. k
12. w
13. s
14. z
15. w
16. d
17. f
18. r
19. t
20. y
21. y
22. u
23. i
24. o
25. k
26. s
27. a
28. t

[//]: # (1.  :fontawesome-brands-readme: Markdown-based file which is rendered when the Git repo is opened in the browser.)

[//]: # (    Usually you would put here short project description, :material-tag: badges and :material-link-plus: links to the docs.)

[//]: # ()
[//]: # (2. :octicons-file-directory-fill-24: Directory with the package-related code.)

[//]: # (   Please note that the folder name contains underscores &#40;`_`&#41; instead of spaces since it should follow Python module naming conventions.)

[//]: # (   <br/>To use the code of this directory locally, we've used <br/> `pip install -e ".[dev]"` command above.)

[//]: # ()
[//]: # (3. In this file you would usually place the package version by using the following syntax:<br>)

[//]: # (   ```python)

[//]: # (   __version__ = "X.Y.Z" # for example 0.0.1)

[//]: # (   ```)

[//]: # (4. This file is generated by `dbx` to provide you useful class `Task` for further task implementations.)

[//]: # (   <br>It contains code that is required to initialize PySpark Session and setup the logging output.)

[//]: # (5. In this folder we store task code - PySpark applications that are to be launched in the local environment or in Databricks workspace.)

[//]: # (6. Note that to be able to import code from the nested folders, an `__init__.py` file should exist. This file could be empty.)

[//]: # (7. In this file we store code with sample ETL task. We'll go through it later.)

[//]: # (8. In this file we store code with sample ML task. We'll go through it later.)

[//]: # (9. Directory where both workflow deployment configurations and task configurations are stored.)

[//]: # (   Deployment configurations are responsible for cluster, dependencies and orchestration configuration,<br>)

[//]: # (   while task configuration files are used to pass parameters to the task.)

[//]: # (10. One of the most important files)

You can :material-cursor-default-click: click on the :material-file: file and :material-folder: folder annotations for in-depth explanations.

