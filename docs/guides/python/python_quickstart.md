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
│   └── workflows
│       ├── onpush.yml #(5)
│       └── onrelease.yml #(6)
├── .gitignore #(7)
├── README.md #(8)
├── charming_aurora #(9)
│   ├── __init__.py #(10)
│   ├── common.py #(11)
│   └── tasks #(12)
│       ├── __init__.py
│       ├── sample_etl_task.py #(13)
│       └── sample_ml_task.py #(14)
├── conf #(15)
│   ├── deployment.yml #(16)
│   └── tasks #(17)
│       ├── sample_etl_config.yml #(18)
│       └── sample_ml_config.yml #(19)
├── notebooks #(20)
│   └── sample_notebook.py
├── pyproject.toml #(21)
├── setup.py #(22)
└── tests #(23)
    └── unit #(24)
        ├── conftest.py #(25)
        └── sample_test.py #(26)
```

1. This is an auxiliary folder, used to store dbx-related information.
   <br/>Please don't store your files in this folder. The `.dbx/project.json` file shall also be provided in your git repository, don't add it to `.gitignore`.
2. This file used to store information about currently used `ExecutionContext` while running `dbx execute`. It's expected to ignore this file in `.gitignore`.
3. Project file is used to keep information about various environments and project-wide settings. It shouldn't be ignored when using git, since it points to the specific environment settings. [Read more about this file here]().
4. This folder is GitHub Actions-specific. It's used to store CI/CD pipeline defitions under `workflows` section. If you're not going to use GitHub Actions, you can delete it.
5. This file is GitHub Actions-specific. It defines the CI pipeline that will be executed on each push to the git repository. You can read more about the CI pipelines [here]().
6. This file is GitHub Actions-specific. It defines the CD pipeline that will be executed on each **tag** push to the repository (usually a new release is marked with a tag). You can read more about the CD pipelines [here]().
7. Gitignore keeps specific files or folders out of git. Very useful to ignore aux. folders (e.g. `dist`), as well as secret files out of repo. Read more about `.gitignore` [here](https://git-scm.com/docs/gitignore).
8. :fontawesome-brands-readme: Markdown-based file which is rendered when the Git repo is opened in the browser.
   Usually you would put here short project description, :material-tag: badges and :material-link-plus: links to the docs.
9. :octicons-file-directory-fill-24: Directory with the package-related code.
   Please note that the folder name contains underscores (`_`) instead of spaces since it should follow Python module naming conventions.
   <br/>To use the code of this directory locally, we've used <br/> `pip install -e ".[dev]"` command above.
10. In this file you would usually place the package version by using the following syntax:<br>
   ```python
   __version__ = "X.Y.Z" # for example 0.0.1
   ```
11. This file is generated by `dbx` to provide you useful class `Task` for further task implementations.
    <br>It contains code that is required to initialize PySpark Session and setup the logging output.
12. In this folder we store task code - PySpark applications that are to be launched in the local environment or in Databricks workspace.
13. In this file we store code with sample ETL task. We'll go through it later.
14. In this file we store code with sample ML task. We'll go through it later.
15. Directory where both workflow deployment configurations and task configurations are stored.
    Deployment configurations are responsible for cluster, dependencies and orchestration configuration,<br>
    while task configuration files are used to pass parameters to the task.
16. One of the most important files - the deployment configuration file.<br/>
    It defines workflows and their respective parameters and cluster configurations. Please read more about this file in the [reference section]()
17. In this folder we store task argument configuration files - these files specify parameters that are being passed into the tasks when they're deployed and launched.
18. This file contains ETL task configurations, we'll touch on it later.
19. This file contains ML task configurations, we'll touch on it later.
20. In this folder you could find a sample notebook that shows how to use dbx-written code from a Databricks Repos.
21. Python Project configuration file. Could contain tool configurations. Read more about this file [here](https://pip.pypa.io/en/stable/reference/build-system/pyproject-toml/).
22. This file also specifies project configurations such as dependencies and other project characteristics. Please find the documentation of this file [here](https://setuptools.pypa.io/en/latest/userguide/index.html), and [here](https://the-hitchhikers-guide-to-packaging.readthedocs.io/en/latest/quickstart.html) is another good guide.
23. Folder where we keep the `tests` relevant to this project. There is no strict requirement to store tests in this folder.
24. In the future you might consider also having a set of `integration` or `e2e` tests, so we keep the unit tests separately. Please read more on the testing concepts [here]().
25. This file contains useful mocks (e.g. Spark, MLflow) and fixtures for testing. Read more on this file [here](https://docs.pytest.org/en/6.2.x/fixture.html).
26. This file contains local tests for sample workflow. We're going to discover its content later

You can :material-cursor-default-click: click on the :material-plus-circle-outline: annotations for in-depth explanations.

