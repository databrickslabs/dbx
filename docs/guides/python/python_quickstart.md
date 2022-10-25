# :material-language-python: :material-airplane-takeoff: Python quickstart

In this guide we're going to walk through a typical setup for development purposes.

In the end of this guide you'll have a prepared local environment, as well as capabilities to:

* Run local unit tests
* Deploy workflows to Databricks
* Launch workflows on the Databricks platform

!!! tip "Other CI and Git providers"

    Although this example is based on [GitHub](https://github.com/) and [GitHub Actions](https://github.com/features/actions),
    `dbx` can be easily and seamlessly used with any CI provider and any Git provider.<br/>
    The built-in Python template also can generate pipelines for [Azure DevOps](https://azure.microsoft.com/en-us/products/devops/) and [GitLab CI](https://docs.gitlab.com/ee/ci/).

## :fontawesome-solid-list-check: Prerequisites

- :material-github: GitHub account
- :material-git: Git on your local machine
- Conda and :material-language-python: Python 3.8+ on your local machine
- 🧱 Databricks workspace you can use

## :material-cog-play-outline: Preparing the local environment

In this example we'll call our project: **:fontawesome-solid-wand-magic-sparkles: charming-aurora**.

Create a new conda environment:

```bash
conda create -n charming-aurora python=3.9
```

Activate this environment:

```bash
conda activate charming-aurora
```

Install OpenJDK for local Apache Spark tests:

```bash
conda install -c conda-forge openjdk=11.0.15
```

Install `dbx`:

```bash
pip install dbx
```

Create a new [Databricks API token](https://docs.databricks.com/dev-tools/api/latest/authentication.html) and
   configure the CLI:

```bash
databricks configure --profile charming-aurora --token
```

Verify that profile is working as expected:

```bash
databricks --profile charming-aurora workspace ls /
```
Now the preparation is done, let’s generate a project skeleton using [`dbx init`](../../reference/cli.md):

```bash
dbx init \
    -p "cicd_tool=GitHub Actions" \
    -p "cloud=<your-cloud>" \
    -p "project_name=charming-aurora" \
    -p "profile=charming-aurora" \
    --no-input
```

!!! warning "Choosing the artifact storage wisely"

    Although for the quickstart we're using the standard `dbfs://`-based artifact location, it's **strictly recommended** to use
    proper cloud-based storage for artifacts. Please read more in [this section](../../concepts/artifact_storage.md).

    To change the artifact location for this specific guide, add the following parameter to the command above:
    ```bash
    dbx init \
    -p "cicd_tool=GitHub Actions" \
    -p "cloud=<your-cloud>" \
    -p "project_name=charming-aurora" \
    -p "profile=charming-aurora" \
    -p "artifact_location=<s3://some/path OR wasbs://some/path OR gs://some/path>"
    --no-input
    ```

Step into the newly generated folder:

```bash
cd charming-aurora
```

Install dependencies for local environment:

```bash
pip install -e ".[local,test]"
```

???- info ":fontawesome-brands-windows: Windows-specific local configurations"

    In some setup cases on the Windows machines you might run into various issues. Here is a quick writeup on how to handle some of them:

    1. Abscence of `winutils.exe`. This error will be visible in the output as follows:
    ```
    WARN Shell: Did not find winutils.exe: java.io.FileNotFoundException
    ```
    An additional message about HADOOP_HOME not being defined also appears.
    In case if you're running into this, please install winutils as described [here](https://github.com/cdarlint/winutils).

    2. Issues with pyspark worker communication:
    ```
    Python worker failed to connect back
    ```
    Use the [`findspark`](https://github.com/minrk/findspark) package in your local unit tests to correctly identify Apache Spark on the local path.

After installing all the dependencies, it's time to run local unit tests:

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

Tada! :partying_face:. Your first project has been created and successfully tested 🎉.

Since we have the package installed and tests running, let's take a closer look at the project structure.

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
    ├── entrypoint.py #(24)
    ├── integration
    │   └── e2e_test.py
    └── unit #(25)
        ├── conftest.py #(26)
        └── sample_test.py #(27)
```

1. This is an auxiliary :material-folder-cog: folder, used to store dbx-related information.
   <br/>Please don't store your files in this folder. The `.dbx/project.json` file shall also be provided in your git repository, don't add it to `.gitignore`.
2. This file used to store information about currently used `ExecutionContext` while running `dbx execute`. It's expected to ignore this file in `.gitignore`.
3. Project file is used to keep information about various environments and project-wide settings. It shouldn't be ignored when using git, since it points to the specific environment settings. [Read more about this file here](../../reference/project.md).
4. :fontawesome-brands-square-github: This folder is GitHub Actions-specific. It's used to store CI/CD pipeline defitions under `workflows` section. If you're not going to use GitHub Actions, you can delete it.
5. :fontawesome-brands-square-github: This file is GitHub Actions-specific. It defines the :octicons-repo-forked-24: CI pipeline that will be executed on each push to the git repository. You can read more about the CI pipelines [here](../../concepts/devops.md#continuous-integration-and-continuous-delivery).
6. :fontawesome-brands-square-github: This file is GitHub Actions-specific. It defines the :octicons-repo-forked-24: CD pipeline that will be executed on each **tag** push to the repository (usually a new release is marked with a tag). You can read more about the CD pipelines [here](../../concepts/devops.md#continuous-integration-and-continuous-delivery).
7. Gitignore keeps specific files or folders out of git. Very useful to ignore aux. folders (e.g. `dist`), as well as secret files out of repo. Read more about `.gitignore` [here](https://git-scm.com/docs/gitignore).
8. :fontawesome-brands-readme: Markdown-based file which is rendered when the Git repo is opened in the browser.
   Usually you would put here short project description, :material-tag: badges and :material-link-plus: links to the docs.
9. :octicons-file-directory-fill-24: Directory with the package-related code.
   Please note that the folder name contains underscores (`_`) instead of spaces since it should follow Python module naming conventions.
   <br/>To use the code of this directory locally, we've used <br/> `pip install -e ".[local,test]"` command above.
10. In this file you would usually place the package version by using the following syntax:<br>
   ```python
   __version__ = "X.Y.Z" # for example 0.0.1
   ```
11. This file is generated by `dbx` to provide you useful class `Task` for further task implementations.
    <br>It contains code that is required to initialize PySpark Session and setup the logging output.
12. In this folder we store task code - PySpark applications that are to be launched in the local environment or in Databricks workspace.
13. In this file we store code with sample :octicons-package-dependents-24: ETL task. We'll go through it later.
14. In this file we store code with sample :octicons-package-dependents-24: ML task. We'll go through it later.
15. :octicons-file-directory-24: Directory where both workflow deployment configurations and task configurations are stored.
    Deployment configurations are responsible for cluster, dependencies and orchestration configuration,<br>
    while task configuration files are used to pass parameters to the task.
16. :material-file-cog-outline: One of the most important files - the deployment configuration file.<br/>
    It defines workflows and their respective parameters and cluster configurations. Please read more about this file in the [reference section](../../reference/deployment.md)
17. :octicons-file-directory-24: In this folder we store task argument configuration files - these files specify parameters that are being passed into the tasks when they're deployed and launched.
18. :material-cog-box: This file contains ETL task configurations, we'll touch on it later.
19. :material-cog-box: This file contains ML task configurations, we'll touch on it later.
20. :material-notebook-multiple: In this folder you could find a sample notebook that shows how to use dbx-written code from a Databricks Repos.
21. :material-language-python: Python configuration file. Could contain tool configurations. Read more about this file [here](https://pip.pypa.io/en/stable/reference/build-system/pyproject-toml/).
22. :material-language-python: This file specifies Python project configurations such as dependencies and other project characteristics. Please find the documentation of this file [here](https://setuptools.pypa.io/en/latest/userguide/index.html), and [here](https://the-hitchhikers-guide-to-packaging.readthedocs.io/en/latest/quickstart.html) is another good guide.
23. :fontawesome-solid-flask-vial: Folder where we keep the `tests` relevant to this project. There is no strict requirement to store tests in this folder.
24. This file is an entrypoint for integration tests. Read more on the topic of integrations tests [here](./integration_tests.md)
25. :fontawesome-solid-vial-circle-check: In the future you might consider also having a set of `integration` or `e2e` tests, so we keep the unit tests separately. Please read more on the testing concepts [here](../../concepts/testing.md).
26. :material-auto-fix: This file contains useful mocks (e.g. Spark, MLflow) and fixtures for testing. Read more on this file [here](https://docs.pytest.org/en/6.2.x/fixture.html).
27. This file contains local tests for sample :octicons-workflow-24: workflow. We're going to discover its content later

You can :material-cursor-default-click: click on the :material-plus-circle-outline: annotations for in-depth explanations.

Although :material-file-tree: project structure might seem a bit 🤯 overwhelming at the first glance, it's not that complex in real-life usage.

Let's continue our :material-airplane: journey to the part of writing some code.
Open the project in your favourite IDE and set it to use the conda environment we've created above.

## :material-file-edit: Editing tasks code

As it was shown above, our main task code is stored in the `charming_aurora/tasks/*.py` files.

Let's take a look at the content of the ETL task first:

```python title="charming_aurora/tasks/sample_etl_task.py"
from charming_aurora.common import Task
from sklearn.datasets import fetch_california_housing
import pandas as pd


class SampleETLTask(Task):
    def _write_data(self):
        db = self.conf["output"].get("database", "default")
        table = self.conf["output"]["table"]
        self.logger.info(f"Writing housing dataset to {db}.{table}")
        _data: pd.DataFrame = fetch_california_housing(as_frame=True).frame
        df = self.spark.createDataFrame(_data)
        df.write.format("delta").mode("overwrite").saveAsTable(f"{db}.{table}")
        self.logger.info("Dataset successfully written")

    def launch(self):
        self.logger.info("Launching sample ETL task")
        self._write_data()
        self.logger.info("Sample ETL task finished!")

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    task = SampleETLTask()
    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()
```

In this task code, we're importing a `Task` - abstract class generated by `dbx init` for you.
This class wraps all the code relevant for Spark, DBUtils and Logger initialization. It also contains code to read configuration from a file.

To create a new task, we'll only need to create a subclass and implement the `launch` method of the class `Task`.
By default, `Task` class provides useful attributes that are available to use from `self.` methods:

```python
from charming_aurora.common import Task

class SomeTask(Task):
   def launch(self):
      self.spark.range(100) # self.spark provides access to SparkSession object instance
      self.dbutils(...) # self.dbutils provide access to DBUtils
      self.logger.info("some msg") # access to Spark-level driver logger
      self.conf.get("database") # access to the configuration dict which is coming from a conf file or from a dict object in tests

```

Please note that in local tests `self.dbutils` ([DBUtils](https://docs.databricks.com/dev-tools/databricks-utils.html) object on Databricks) is mocked with fixture that is stored in `tests/unit/conftest.py`.

Now you can edit the code of this task to any extent.

## :material-cog-play: Running unit tests

Take a look at the following file:
```python title="tests/unit/sample_test.py"
from charming_aurora.tasks.sample_etl_task import SampleETLTask
from charming_aurora.tasks.sample_ml_task import SampleMLTask
from pyspark.sql import SparkSession
from pathlib import Path
import mlflow
import logging

def test_jobs(spark: SparkSession, tmp_path: Path):
    logging.info("Testing the ETL job")
    common_config = {"database": "default", "table": "sklearn_housing"}
    test_etl_config = {"output": common_config}
    etl_job = SampleETLTask(spark, test_etl_config)
    etl_job.launch()
    table_name = f"{test_etl_config['output']['database']}.{test_etl_config['output']['table']}"
    _count = spark.table(table_name).count()
    assert _count > 0
    logging.info("Testing the ETL job - done")

    logging.info("Testing the ML job")
    test_ml_config = {
        "input": common_config,
        "experiment": "/Shared/charming-aurora/sample_experiment"
    }
    ml_job = SampleMLTask(spark, test_ml_config)
    ml_job.launch()
    experiment = mlflow.get_experiment_by_name(test_ml_config['experiment'])
    assert experiment is not None
    runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
    assert runs.empty is False
    logging.info("Testing the ML job - done")
```

As it comes from the code, we can import the `SampleETLTask` class and run a test directly on it.

To do so, we'll need to provide a `SparkSession` object - and it comes directly into our test from a fixture defined in the `tests/unit/conftest.py`:

We can now call this task with custom parameters and launch it programmatically, verifying the task output:
```python title="tests/unit/sample_test.py"
# imports are omitted
def test_jobs(spark: SparkSession, tmp_path: Path):
    logging.info("Testing the ETL job")
    common_config = {"database": "default", "table": "sklearn_housing"}
    test_etl_config = {"output": common_config}
    etl_job = SampleETLTask(spark, test_etl_config)
    etl_job.launch()
    table_name = f"{test_etl_config['output']['database']}.{test_etl_config['output']['table']}"
    _count = spark.table(table_name).count()
    assert _count > 0
    logging.info("Testing the ETL job - done")
    # code of the ML task test is omitted
```

To launch the tests use the following command:
```bash
pytest tests/unit
```
This command will start local Apache Spark session, run the tests and shutdown afterwards.

To add coverage to the shell output, run the command with the following flag:
```bash
pytest tests/unit --cov
```

Sometimes it's required to export coverage results in various formats, e.g. `html` or `xml`. This could be done via:

```bash
pytest tests/unit --cov --cov-report=html
pytest tests/unit --cov --cov-report=xml
```

With the example above we're now able to run the unit tests and see how good our code is covered.

## :fontawesome-solid-flask-vial: Running integration tests

To run integration tests on the Databricks clusters, take a look [here](./integration_tests.md).

## :material-file-code: Deployment configuration

To launch the code on an all-purpose cluster or to deploy it as a workflow, we'll need to define the workflow
and tasks in the [:material-file-code: deployment file](../../reference/deployment.md). After project generation it looks like this:
```yaml title="conf/deployment.yml" hl_lines="26"
# Custom section is used to store configurations that might be repetative.
# Please read YAML documentation for details on how to use substitutions and anchors.
custom:
  basic-cluster-props: &basic-cluster-props
    spark_version: "10.4.x-cpu-ml-scala2.12"

  basic-static-cluster: &basic-static-cluster
    new_cluster:
      <<: *basic-cluster-props
      num_workers: 1
      node_type_id: "<some-node-type-id" # this value will be different depending on your cloud provider

environments:
  default:
    workflows:
      #######################################################################################
      # this is an example job with single ETL task based on 2.1 API and wheel_task format #
      ######################################################################################
      - name: "charming-aurora-sample-etl"
        tasks:
          - task_key: "main"
            <<: *basic-static-cluster
            python_wheel_task:
              package_name: "charming_aurora"
              entry_point: "etl" # take a look at the setup.py entry_points section for details on how to define an entrypoint
              parameters: ["--conf-file", "file:fuse://conf/tasks/sample_etl_config.yml"]
```

The cluster and task definitions are following the [Databricks Jobs API](https://docs.databricks.com/dev-tools/api/latest/jobs.html), but with some minor alterations.

As you can see, we don't add any package to the `libraries` section of the job.
This is not required, since `dbx` by default will automatically build and add the core package to the tasks definitions.

You might also notice that there is a file reference on the highlighted line. Please learn more about file referencing [here](../../features/file_references.md).
As of now, think of a file reference as a mechanism that allows you to
upload local files to the Databricks workspace in automated fashion and properly resolve them in the workflow definition.

In this specific case, the file reference points to a YAML file with task configuration (not to be confused with deployment configuration).
The logic of parsing the arguments is defined in the `charming_aurora.common.Task` object:
```python title="charming_aurora/common.py"
# some lines were intentionally omitted

class Task(ABC):

    def __init__(self, spark=None, init_conf=None):
        self.spark = self._prepare_spark(spark)
        self.logger = self._prepare_logger()
        self.dbutils = self.get_dbutils()
        if init_conf: #(1)
            self.conf = init_conf
        else:
            self.conf = self._provide_config()
        self._log_conf()

    def _provide_config(self):
        self.logger.info("Reading configuration from --conf-file job option")
        conf_file = self._get_conf_file()
        if not conf_file:
            self.logger.info(
                "No conf file was provided, setting configuration to empty dict."
                "Please override configuration in subclass init method"
            )
            return {}
        else:
            self.logger.info(f"Conf file was provided, reading configuration from {conf_file}")
            return self._read_config(conf_file)

    @staticmethod
    def _get_conf_file(): #(2)
        p = ArgumentParser()
        p.add_argument("--conf-file", required=False, type=str)
        namespace = p.parse_known_args(sys.argv[1:])[0]
        return namespace.conf_file

    @staticmethod
    def _read_config(conf_file) -> Dict[str, Any]:
        config = yaml.safe_load(pathlib.Path(conf_file).read_text()) #(3)
        return config
```

1. This `if`-switch defines whenever the passed argument will be used (as it's done in test), or a default approach based on reading the config file will be used.
2. You can override this method or create a new one if you would like to pass more parameters than just `--conf-file`.
3. Since a file-system `safe_load` is used, we're passing the file reference by using `file:fuse://` syntax. Read more about it [here](../../features/file_references.md)

You can :material-cursor-default-click: click on the :material-plus-circle-outline: annotations for in-depth explanations.

## :material-cloud-upload-outline: Executing code on Databricks

Since we have now our task defined, we can execute this code on Databricks. There are two fundamentally different Databricks cluster types:

* All-purpose (sometimes called interactive) clusters
* Job (sometimes called automated) clusters

All-purpose clusters are an excellent choice for interactive workloads (and they're not really suitable for scheduled or automated launches).

To execute the code of the ETL task on the all-purpose cluster, do the following:
```bash
dbx execute charming-aurora-sample-etl --task=main --cluster-name="some-interactive-cluster-name"
```

!!! tip

    If cluster is stopped, it will be automatically started by the execute command.


This task will build the package, upload it to the DBFS and run it in a separate context.
Other users who are working with the same package  on the same cluster won't be affected.

The execution output will be returned to the shell, but you won't see the `self.logger` statements.
The reason is that `self.logger` logs it's content to the Driver Logs, which is visible in the Cluster UI.

!!! tip

    If you don't like the interface of the Cluster Driver Logs UI, consider connecting to the cluster WebTerminal and
    running this command:
    ```
    tail -f /databricks/driver/logs/log4j-active.log
    ```
    This command will provide you realtime logs, but logs of all other actions will be also visible.
    Another option would be to setup logs export to cloud native services (e.g. [Azure Log Analytics](https://azure.microsoft.com/en-us/services/monitor), [AWS CloudWatch](https://aws.amazon.com/cloudwatch/)),
    or to specialized logging solutions like [Datadog](https://www.datadoghq.com/) and [New Relic](https://newrelic.com/).


With a successfully executed code on the all-purpose cluster, we can now move on to the deployment and launch.

## :material-door-open: Defining the entrypoints

Let's get back to the ETL task to point our attention to the interfaces of how this task will be launched in Databricks:
```python title="charming_aurora/tasks/sample_etl_task.py"
# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    task = SampleETLTask()
    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()
```

The `entrypoint` is a function that will be executed when the package is specified as a `python_wheel_task` in the deployment file.

```yml title="conf/deployment.yml"
# relevant section of the deployment file, some parts are omitted
environments:
  default:
    workflows:
      - name: "charming-aurora-sample-etl"
        tasks:
          - task_key: "main"
            <<: *basic-static-cluster
            python_wheel_task:
              package_name: "charming_aurora"
              entry_point: "etl" # take a look at the setup.py entry_points section for details on how to define an entrypoint
              parameters: ["--conf-file", "file:fuse://conf/tasks/sample_etl_config.yml"]
```

To setup this function to a specific `entry_point` name, we assign this function in the `console_scripts` section of `setup.py` file:

```python title="setup.py"
# irrenevant content is ommited
setup(
   ...,
    entry_points = {
        "console_scripts": [
            "etl = charming_aurora.tasks.sample_etl_task:entrypoint",
            "ml = charming_aurora.tasks.sample_ml_task:entrypoint"
    ]},
    ...
)
```
As it states above, the `entrypoint` function from `sample_etl_task` file will be called when the `entry_point` points to `etl`.

With this setup finished, we now can deploy the workflow.

## :fontawesome-solid-ship: Deploying the workflow

To deploy the workflow as a [Databricks Job](https://docs.databricks.com/workflows/jobs/jobs.html) simply run:
```bash
dbx deploy charming-aurora-sample-etl
```

Now you can go to the Databricks UI and verify that the job has been properly created.

There are cases when the immediate job creation is not needed - in this case `dbx` allows to use another deployment pattern which is described [here](../../features/assets.md).


## :octicons-feed-rocket-16: Launching the workflow

To launch the deployed workflow, use the following command:

```bash
dbx launch charming-aurora-sample-etl
```

You can add `--trace` flag to keep the current shell busy until the Job Run is finished.


## :material-head-lightbulb: Summary


With this guide we've done the following:

- [x] Local development environment is fully prepared
- [x] Unit tests were launched and code coverage was verified
- [x] Workflow definition was provided in the deployment file
- [x] Based on this definition, a task was executed on an all-purpose cluster
- [x] Based on this definition a new Databricks workflow has been created
- [x] This Databricks workflow has been successfully launched

Congratulations 🎉! Now you can easily develop new IDE-based projects with `dbx`.

For more advanced topics, such as CI/CD, dependency management and various guidance please check other documentation sections.

