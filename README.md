# dbx by Databricks Labs

<div>
    <p align="center">
        🚨 Dear users of <code>dbx</code>, please note that this project is <b>no longer actively maintained</b>. 🚨
    </p>
    <p align="center">
        We recommend using <a href="https://docs.databricks.com/en/dev-tools/bundles/index.html">Databricks asset Bundles for CI/CD</a>.
    </p>
    <p align="center">
        If you're looking for a programmatic approach, even more flexible than the existing Jinja-based
        functionality, please check out <a href="https://docs.databricks.com/aws/en/dev-tools/bundles/python">
            Python for Databricks Asset Bundles</a>. 🐍
    </p>
</div>

---

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
    <a href="https://codecov.io/gh/databrickslabs/dbx">
        <img src="https://img.shields.io/codecov/c/github/databrickslabs/dbx?style=for-the-badge&amp;token=S7ADH3W2E3"
             alt="codecov"/>
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

## Documentation

Please refer to the [docs page](https://dbx.readthedocs.io/en/latest/index.html).

## Interface versioning

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

Databricks recommends using [Databricks asset bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html) for CI/CD.
    Please see [migration guidance](https://docs.databricks.com/en/archive/dev-tools/dbx/dbx-migrate.html) on how to migrate from dbx to dabs

## Feedback

Issues with `dbx`? Found a bug? Have a great idea for an addition? Feel
free to file an
[issue](https://github.com/databrickslabs/dbx/issues/new/choose).

## Contributing

Please find more details about contributing to `dbx` in the contributing
[doc](https://github.com/databrickslabs/dbx/blob/master/contrib/CONTRIBUTING.md).
