# Frequently asked questions

## Does `dbx` support R?

Unfortunately at the moment `dbx` doesn't support R in `dbx execute` mode.
At the same time you can use `dbx deploy` and `dbx launch` to work with R-based notebook tasks.

## Does `dbx` support `poetry`?

Yes, setup the build logic for poetry as described [here](./features/build_management.md).

## Does `dbx` support `flit`?

Yes, setup the build logic for flit as described [here](./features/build_management.md).

## What's the difference between `dbx execute` and `dbx launch`?

The `dbx execute` command runs your code on `all-purpose` [cluster](./concepts/cluster_types.md#all-purpose-clusters).
It's very handy for interactive development and data exploration.

!!! danger "Don't use `dbx execute` for production workloads"

    It's not recommended to use `dbx execute` for production workloads. Run your workflows on the dedicated job clusters instead.
    Reasoning is described in detail in the [concepts section](../concepts/cluster_types).

In contrast to the `dbx execute`, `dbx launch` launches your workflow on a [dedicated job cluster](./concepts/cluster_types.md#job-clusters). This is a recommended way for CI pipelines, automated launches etc.

When in doubt, follow the [summary section](./concepts/cluster_types.md#summary) for precise guidance.

## Can I have multiple `deployment files` ?
Yes, `dbx deploy` accepts `--deployment-file PATH` as described [here](./reference/cli.md#dbx-deploy).
