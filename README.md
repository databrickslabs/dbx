# dbx by Databricks Labs

<p align="center">
    <a href="https://dbx.readthedocs.io/">
        <img src="https://raw.githubusercontent.com/databrickslabs/dbx/master/images/logo.svg" class="align-center" width="200" height="200" alt="logo" />
    </a>
</p>

<p align="center">
    <b>🧱Databricks CLI eXtensions - aka <code>dbx</code> is a CLI tool for development and advanced Databricks workflows management.</b>
</p>

---

<p align="center">
    <a href="https://dbx.readthedocs.io/en/latest/?badge=latest">
        <img src="https://img.shields.io/readthedocs/dbx?style=for-the-badge" alt="Documentation Status"/>
    </a>
    <a href="https://pypi.org/project/dbx/">
        <img src="https://img.shields.io/pypi/v/dbx?color=green&amp;style=for-the-badge" alt="Latest Python Release"/>
    </a>
    <a href="https://github.com/databrickslabs/dbx/actions/workflows/onpush.yml">
        <img src="https://img.shields.io/github/workflow/status/databrickslabs/dbx/build/main?style=for-the-badge"
             alt="GitHub Workflow Status (branch)"/>
    </a>
    <a href="https://codecov.io/gh/databrickslabs/dbx">
        <img src="https://img.shields.io/codecov/c/github/databrickslabs/dbx?style=for-the-badge&amp;token=S7ADH3W2E3"
             alt="codecov"/>
    </a>
    <a href="https://lgtm.com/projects/g/databrickslabs/dbx/alerts">
        <img src="https://img.shields.io/lgtm/alerts/github/databrickslabs/dbx?style=for-the-badge" alt="lgtm-alerts"/>
    </a>
    <a href="https://lgtm.com/projects/g/databrickslabs/dbx/context:python">
        <img src="https://img.shields.io/lgtm/grade/python/github/databrickslabs/dbx?style=for-the-badge"
             alt="lgtm-code-quality"/>
    </a>
    <a href="https://pypistats.org/packages/dbx">
        <img src="https://img.shields.io/pypi/dm/dbx?style=for-the-badge" alt="downloads"/>
    </a>
    <a href="https://github.com/psf/black">
        <img src="https://img.shields.io/badge/code%20style-black-000000.svg?style=for-the-badge"
             alt="We use black for formatting"/>
    </a>
</p>

---

## Concept

`dbx` simplifies Databricks workflows development, deployment and launch across multiple
environments. It also helps to package your project and deliver it to
your Databricks environment in a versioned fashion. Designed in a
CLI-first manner, it is built to be actively used both inside CI/CD
pipelines and as a part of local tooling for rapid prototyping.

## Requirements

- Python Version \> 3.8
- `pip` or `conda`

## Installation

- with `pip`:

```
pip install dbx
```

## Quickstart

Please refer to the [Quickstart section](https://dbx.readthedocs.io/en/latest/quickstart.html).

## Documentation

Please refer to the [docs page](https://dbx.readthedocs.io/en/latest/index.html).

## Differences from other tools

| Tool                                                                                             | Comment                                                                                                                                                                                                                                                                           |
|--------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [databricks-cli](https://github.com/databricks/databricks-cli)                                   | dbx is NOT a replacement for databricks-cli. Quite the opposite - dbx is heavily dependent on databricks-cli and uses most of the APIs exactly from databricks-cli SDK.                                                                                                           |
| [mlflow cli](https://www.mlflow.org/docs/latest/cli.html)                                        | dbx is NOT a replacement for mlflow cli. dbx uses some of the MLflow APIs under the hood to store serialized job objects, but doesn't use mlflow CLI directly.                                                                                                                    |
| [Databricks Terraform Provider](https://github.com/databrickslabs/terraform-provider-databricks) | While dbx is primarily oriented on versioned job management, Databricks Terraform Provider provides much wider set of infrastructure settings. In comparison, dbx doesn't provide infrastructure management capabilities, but brings more flexible deployment and launch options. |
| [Databricks Stack CLI](https://docs.databricks.com/dev-tools/cli/stack-cli.html)                 | Databricks Stack CLI is a great component for managing a stack of objects. dbx concentrates on the versioning and packaging jobs together, not treating files and notebooks as a separate component.                                                                              |

## Limitations

- Development:
    - `dbx` currently doesn't provide interactive debugging
      capabilities.
      If you want to use interactive debugging, you can use [Databricks
      Connect](https://docs.databricks.com/dev-tools/databricks-connect.html) +
      `dbx` for deployment operations.

    - `dbx execute` only supports Python-based projects which use
      `spark_python_task` or `python_wheel_task`. Notebooks or Repos are
      not supported in `dbx execute`.

    - `dbx execute` can only be used on clusters with Databricks ML
      Runtime 7.X or higher.
- General:
    - `dbx` doesn't support [Delta Live
      Tables](https://databricks.com/product/delta-live-tables) at the
      moment.

## Versioning

For CLI interfaces, we support [SemVer](https://semver.org/) approach.
However, for API components we don't use SemVer as of now. This may lead
to instability when using `dbx` API methods directly.

## Legal Information

This software is provided as-is and is not officially supported by
Databricks through customer technical support channels. Support,
questions, and feature requests can be communicated through the Issues
page of this repo. Please see the legal agreement and understand that
issues with the use of this code will not be answered or investigated by
Databricks Support.

## Feedback

Issues with `dbx`? Found a bug? Have a great idea for an addition? Feel
free to file an
[issue](https://github.com/databrickslabs/dbx/issues/new/choose).

## Contributing

Please find more details about contributing to `dbx` in the contributing
[doc](https://github.com/databrickslabs/dbx/blob/master/contrib/CONTRIBUTING.md).
