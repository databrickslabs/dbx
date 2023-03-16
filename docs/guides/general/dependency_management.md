# :material-package-variant-plus: Dependency management

This section describes various approaches to the dependency management.

In general, this section is relevant for [package-based Python modules](../python/devloop/package.md) and JVM projects that use [this](../jvm/jvm_devloop.md) development loop approach.

At the same time, some concepts provided here could be also used for :material-notebook-heart: pure notebook or [mixed-mode projects](../python/devloop/mixed.md).

## :material-language-python: Python dependency management

:simple-databricks: Databricks supports various ways to provide and supply the [Python dependencies](https://docs.databricks.com/libraries/index.html).
We'll concentrate on the approaches that are well-compatible with `dbx` and describe them in detail for [various cluster types](../../concepts/cluster_types.md).

### :material-lightning-bolt-circle: All-purpose clusters and dependency management in the development loop

One of the most common ways to manage dependencies on the all-purpose clusters is to specify them in the [Cluster Libraries](https://docs.databricks.com/libraries/cluster-libraries.html) section.
This approach is simple, and requires going into the UI to install additional libraries for the whole cluster.

Despite it's simplicity, it could also bring some hurdles depending on your development loops:

* Library becomes cluster-wide available, which means that all users of the same cluster will be forced to use exactly this library version.
* To re-install the library (e.g. install a newer version of it), cluster restart is required.

For [package-based Python projects](../python/devloop/package.md) `dbx` uses a different approach to the dependency management for interactive development loops.
This approach is based on the so-called [Notebook-scoped or Context-scoped libraries](https://docs.databricks.com/libraries/notebooks-python-libraries.html).

Logical flow of this approach is pretty simple:

* Locally a wheel with all dependency references is created
* This wheel is uploaded to the artifact storage
* On the all-purpose cluster, wheel is installed in the user context via `%pip install --force-reinstall <versioned-wheel-path>/package.whl` command

All these steps are executed every time when a `dbx execute` command is launched.

With this approach it's pretty easy to add new dependencies to your wheel and guarantee that the expected version of a library will be used.

### :material-package-variant-closed-check: Managing dependencies for various environments

Important thing to mention is that dependencies might be different depending on the environment where they're required to be installed.

Logically most of the dependencies could be classified as follows:

* Dependencies that are already installed in the :simple-databricks: [DBR](https://docs.databricks.com/release-notes/runtime/releases.html), but might be required locally (e.g. `pyspark`)
* Dependencies that are not provided in the :simple-databricks: [DBR](https://docs.databricks.com/release-notes/runtime/releases.html), but might be required locally and on DBR (e.g. `pytest` for testing runs)
* Main package dependencies that are not provided in the DBR and not provided in the local environment (e.g. if you're doing some ML with imbalanced classes it could be the [`imbalanced-learn`](https://imbalanced-learn.org/stable/install.html#getting-started) library)

Usually library management is done with `setup.py` or [`poetry`](https://github.com/python-poetry/poetry):

=== ":material-toolbox: setuptools-based approach"

    A generic layout of the `setup.py` file could look like this:
    ```python title="setup.py"
    from setuptools import find_packages, setup
    from your_package_name import __version__

    PACKAGE_REQUIREMENTS = ["pyyaml"] #(1)

    LOCAL_REQUIREMENTS = [ #(2)
    "pyspark==3.2.1",
    "delta-spark==1.1.0",
    "scikit-learn",
    "pandas",
    "mlflow",
    ]

    TEST_REQUIREMENTS = [ #(3)
    # development & testing tools
    "pytest",
    "coverage[toml]",
    "pytest-cov",
    "dbx>=0.8"
    ]

    setup(
        name="your_package_name",
        packages=find_packages(exclude=["tests", "tests.*"]),
        setup_requires=["setuptools","wheel"],
        install_requires=PACKAGE_REQUIREMENTS,
        extras_require={"local": LOCAL_REQUIREMENTS, "test": TEST_REQUIREMENTS}, #(4)
        version=__version__,
    )
    ```

    1. Package requirements section describes main requirements that are not expected to be provided in the Databricks Runtime
    2. Local requirements represent libraries that are required for local development and that are not provided on the Databricks Runtime
    3. Test requirements represent dependencies that are required in case when tests are being launched (e.g. CI pipeline).
    4. Read more on the `extras_require` section [here](https://setuptools.pypa.io/en/latest/userguide/dependency_management.html).

    To add a package-wide dependency, simply add another line to the `PACKAGE_REQUIREMENTS` list.

    !!! tip "Dependencies from git repositories"

        Sometimes it might be required to add dependencies from a :simple-git: Git repository. According to the [PEP-508](https://peps.python.org/pep-0508/), the following format is supported:
        ```python
        PACKAGE_REQUIRES = ['some-pkg @ git+ssh://git@github.com/someorgname/pkg-repo-name@v1.1']
        ```
        Where `v1.1` indicates the tag and could be replaced with a branch, commit, or a different type of tag.

=== ":simple-poetry: poetry-based approach"

    In poetry, package management is done by using it's CLI interface.
    Here is how you could add a dev dependency:
    ```bash
    poetry add dbx --dev
    ```
    Here is how you could add a package-wide dependency:
    ```
    poetry add pyyaml
    ```
    And here is how you could add an extra dependency, e.g. dependencies for tests:
    ```
    poetry add pytest -E test
    ```

    !!! tip "Adding dependencies that already exist in DBR"

        It is heavy recommended **not** to add dependencies that are already introduced in the Databricks Runtime as package dependencies.
        Instead, add them as `-E local` or `--dev` dependency to avoid long reinstallation times.

By using the extras to classify the dependencies, you can achieve flexible setup options for various environments (e.g. local and CI).

!!! tip "Installing extras during the `dbx execute`"

    During running your pipelines on the all-purpose clusters via `dbx execute`, you might want to explicitly tell `dbx` to install the specified section of the `extras`.

    To do so, provide the following option to the command:

    ```bash
    dbx execute ... --pip-install-extras="test,other-extra,one-more-extra"
    ```

???- warning "Why not use :material-list-box-outline: `requirements.txt` and why it's deprecated in `dbx`?"

    Although there is a capability to add `--requirements-file` to the execute and deploy command, we **don't recommend** using it for various reasons:

    * There is no proper API to parse the requirements file, so we cannot guarantee that every syntax provided there will be supported
    * Requirements file doesn't support using extras which might be really useful as described above
    * Less files leads to simpler development workflow


### :octicons-zap-16: Job clusters and dependency management

If you are using the approach described above, no additional actions are required.

`dbx` will automatically add your package to the libraries section, which will lead to the package installation at the cluster start.

Since the dependencies were already specified in the package, they will be automatically installed.
There is no need to explicitly provide the dependencies in the `libraries` section of the [:material-file-code: deployment file](../../reference/deployment.md).

## :material-language-java: JVM dependency management

As for JVM-based projects, one of the typical build tools shall be used, such as:

- :simple-apachemaven: [Apache Maven](https://maven.apache.org/)
- :simple-gradle: [Gradle](https://gradle.org/)
- :simple-scala: [Scala Build Tool (sbt)](https://www.scala-sbt.org/)

As well as many others.

While providing dependencies in these tools, keep in mind the following:

- There is no need to explicitly add Apache Spark and Delta Lake dependencies, you can use the `provided` tag for them to avoid jar growing in size.
- Make sure your project is using the same JVM version which is specified in the :simple-databricks: [DBR](https://docs.databricks.com/release-notes/runtime/releases.html)

For interactive development with all-purpose clusters, consider using the [Cluster Libraries](https://docs.databricks.com/libraries/cluster-libraries.html) functionality.

???- warning "Context-scoped JVM libraries on all-purpose clusters"

    Plese note that context-scoped JVM libraries are not supported in DBR.<br/>
    If you need to have a fully detached environment with various versions per different contexts, consider using the [development loop for JVM-based projects](../jvm/jvm_devloop.md).


## :material-inbox-arrow-up: Advanced dependency management patterns

This section describes advanced dependency management patterns.

### :simple-pypi: Installing Python packages from custom Pypi repos

In some cases you might require to install packages from the custom pypi repository, e.g. [Azure Artifacts](https://azure.microsoft.com/en-us/products/devops/artifacts/), [JFrog](https://jfrog.com/) or [Nexus](https://www.sonatype.com/products/nexus-repository).

To achieve that, you can use the init scripts to configure additional pypi repositories.
The [`pip` config file](https://pip.pypa.io/en/stable/user_guide/#config-file) is expected to be provided in `/etc/pip.conf` directory.

You can add additional lines to it via init script, for example:
```bash title="init_scripts/pypi.sh"
echo """[global]
index-url=https://pypi.org/simple
extra-index-url=https://my.custom.pypi.example.com/simple/
""" > /etc/pip.conf
```


### :material-package-variant-closed-plus: Adding JVM dependencies to your Python project

Sometimes you would like to have a maven dependency added to your Python project (e.g. it contains a JVM-based UDF that you plan to call from Python).

On the all-purpose cluster install the library using Cluster Libraries as described [here](https://docs.databricks.com/libraries/cluster-libraries.html).

While deploying the workflow, add the dependency using the `libraries` section of the [:material-file-code: deployment file](../../reference/deployment.md):

```yaml title="conf/deployment.yml"
# some lines are omitted for readability

environments:
  default:
    workflows:
      - name: "workflow-with-mvn-dependencies"
        tasks:
          - task_key: "main"
            <<: *basic-static-cluster
            libraries:
                - maven:
                    coordinates: "org.jsoup:jsoup:1.7.2"
            spark_python_task:
                python_file: "file://some/path/entrypoint.py"
```

Another use-case might be that you have a local `jar` that you would like to add to the deployment configuration.
In this case it's handy to use the [Jinja functionality with custom functions](../../features/jinja_support.md):

```yaml title="conf/deployment.yml"
# some lines are omitted for readability
# please note that inplace jinja support shoud be enabled

environments:
  default:
    workflows:
      - name: "workflow-with-jar-dependencies"
        tasks:
          - task_key: "main"
            <<: *basic-static-cluster
            libraries:
                - jar: "{{ 'file://' + dbx.get_last_modified_file('target/scala-2.12', 'jar') }}" #(1)
                - jar: "file://static/local/file/reference.jar" #(2)
            spark_python_task:
                python_file: "file://some/path/entrypoint.py"
```

1. This approach uses built-in [Jinja functionality with custom functions](../../features/jinja_support.md) to dynamically find the latest jar file in the given folder
2. This approach uses local [file reference](../../features/file_references.md)
