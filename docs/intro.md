# :octicons-feed-rocket-16: dbx by Databricks Labs - intro

<p align="center">
    <a href="https://dbx.readthedocs.io/">
        <img src="https://raw.githubusercontent.com/databrickslabs/dbx/master/images/logo.svg" class="align-center" width="200" height="200" alt="logo" />
    </a>
</p>

🧱 Databricks CLI eXtensions - aka `dbx` is a CLI tool for development and advanced Databricks workflows management.

## :octicons-light-bulb-24: Concept

`dbx` aims to improve development experience for Data and ML teams that use Databricks, by providing the following capabilities:

- Ready to use project templates, with built-in support to use custom templates
- Simple configuration for multi-environment setup
- Interactive development loop for Python-based projects
- Flexible deployment configuration
- Built-in versioning for deployments

Since `dbx` primary interface is CLI, it's easy to use it in various CI/CD pipelines, independent of the CI provider.

Read more about the place of `dbx` and potential use-cases in the [ecosystem section](concepts/ecosystem.md).

## :thinking: Differences from other tools

| Tool                                                                                             | Comment                                                                                                                                                                                                                                                                           |
|--------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [databricks-cli](https://github.com/databricks/databricks-cli)                                   | dbx is NOT a replacement for databricks-cli. Quite the opposite - dbx is heavily dependent on databricks-cli and uses most of the APIs exactly from databricks-cli SDK.                                                                                                           |
| [mlflow cli](https://www.mlflow.org/docs/latest/cli.html)                                        | dbx is NOT a replacement for mlflow cli. dbx uses some of the MLflow APIs under the hood to store serialized job objects, but doesn't use mlflow CLI directly.                                                                                                                    |
| [Databricks Terraform Provider](https://github.com/databrickslabs/terraform-provider-databricks) | While dbx is primarily oriented on versioned job management, Databricks Terraform Provider provides much wider set of infrastructure settings. In comparison, dbx doesn't provide infrastructure management capabilities, but brings more flexible deployment and launch options. |
| [Databricks Stack CLI](https://docs.databricks.com/dev-tools/cli/stack-cli.html)                 | Databricks Stack CLI is a great component for managing a stack of objects. dbx concentrates on the versioning and packaging jobs together, not treating files and notebooks as a separate component.                                                                              |

Read more about the differences between `dbx` and other instruments in the [ecosystem section](concepts/ecosystem.md).

## :octicons-link-external-24: Next steps

Depending on your developer journey and overall tasks, you might use `dbx` in various ways:

=== ":material-language-python: Python"

    | :material-sign-direction: Developer journey                                                     | :octicons-link-24: Link                            | :octicons-tag-24: Tags                                                                                                                                                                         |
    |-------------------------------------------------------------------------------------------------|----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | Develop a new Python project on Databricks solely in IDE without using Notebooks                | [Python quickstart](./guides/python/python_quickstart.md)                                  | <div class="nowrap">:fontawesome-brands-python: Python</div><div class="nowrap">:fontawesome-solid-laptop: IDE</div>                                                                           |
    | Develop a new Python project on Databricks with Databricks Notebooks and partially in the IDE   | [Python quickstart](./guides/python/python_quickstart.md) followed by<br/> [Mixed-mode development loop for Python projects](./guides/python/devloop/mixed.md)    | <div class="nowrap">:fontawesome-brands-python: Python</div><div class="nowrap">:fontawesome-solid-laptop: IDE</div> <div class="nowrap">:material-notebook-heart-outline: Notebook</div>      |
    | Organize a development loop for an existing Notebooks-based project together with IDE           | [Mixed-mode development loop for Python projects](./guides/python/devloop/mixed.md)    | <div class="nowrap">:fontawesome-brands-python: Python</div><div class="nowrap">:fontawesome-solid-laptop: IDE</div> <div class="nowrap">:material-notebook-heart-outline: Notebook</div>      |
    | Organize a development loop for an existing Python package-based project                        | [Development loop for Python package-based projects](./guides/python/devloop/package.md) | <div class="nowrap">:fontawesome-brands-python: Python</div><div class="nowrap">:fontawesome-solid-laptop: IDE</div> <div class="nowrap"> :octicons-package-16: Packaging</div>                |
    | Add workflow deployment and automation capabilities to an existing Python package-based project | [DevOps for Python package-based projects](./guides/python/devops/package.md)           | <div class="nowrap">:fontawesome-brands-python: Python</div><div class="nowrap"> :octicons-package-16: Packaging</div> <div class="nowrap">:fontawesome-solid-ship: Deployment</div>           |
    | Add workflow deployment and automation capabilities to an existing Notebooks-based project      | [DevOps for Notebooks-based projects](./guides/python/devops/notebook.md)                | <div class="nowrap">:fontawesome-brands-python: Python</div><div class="nowrap">:material-notebook-heart-outline: Notebook</div> <div class="nowrap">:fontawesome-solid-ship: Deployment</div> |

=== ":material-language-java: Java and Scala"

    | :material-sign-direction: Developer journey                                                     | :octicons-link-24: Link                            | :octicons-tag-24: Tags                                                                                                                                                                         |
    |-------------------------------------------------------------------------------------------------|----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | Organize a development loop for an existing JVM-based project (e.g. Java or Scala) in IDE       | [Development loop for JVM-based projects](./guides/jvm/jvm_devloop.md)            | <div class="nowrap">:fontawesome-brands-java: JVM</div><div class="nowrap">:fontawesome-solid-laptop: IDE</div>                                                                                |
    | Add workflow deployment and automation capabilities to an existing JVM-based project            | [DevOps for JVM-based projects](./guides/jvm/jvm_devops.md)                      | <div class="nowrap">:fontawesome-brands-java: JVM</div><div class="nowrap"> :octicons-package-16: Packaging</div><div class="nowrap">:fontawesome-solid-ship: Deployment</div>                 |


## :octicons-stop-24: Limitations

- For interactive development `dbx` can only be used for Python and JVM-based projects.
  Please note that development workflow for JVM-based projects is different from the Python ones.
  For R-based projects, `dbx` can only be used as a deployment management and workflow launch tool.
- `dbx` currently doesn't provide interactive debugging capabilities.
  If you want to use interactive debugging, you can use [Databricks
  Connect](https://docs.databricks.com/dev-tools/databricks-connect.html), and then use
  `dbx` for deployment operations.
- [Delta Live
  Tables](https://databricks.com/product/delta-live-tables) are supported for deployment and launch. The interactive execution mode is not supported. Please read more on DLT with `dbx` in [this guide](guides/general/delta_live_tables.md).

## :octicons-law-24: Legal Information

!!! danger "Support notice"

    This software is provided as-is and is not officially supported by
    Databricks through customer technical support channels. Support, questions, and feature requests can be communicated through the Issues
    page of the [dbx repo](https://github.com/databrickslabs/dbx/issues). Please see the legal agreement and understand that
    issues with the use of this code will not be answered or investigated by
    Databricks Support.

## :octicons-comment-24: Feedback

Issues with `dbx`? Found a :octicons-bug-24: bug?
Have a great idea for an addition? Want to improve the documentation? Please feel
free to file an [issue](https://github.com/databrickslabs/dbx/issues/new/choose).

## :fontawesome-solid-user-plus: Contributing

Please find more details about contributing to `dbx` in the contributing
[doc](https://github.com/databrickslabs/dbx/blob/master/contrib/CONTRIBUTING.md).
