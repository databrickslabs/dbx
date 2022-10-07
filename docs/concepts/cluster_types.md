# :fontawesome-solid-microchip: Cluster types

Databricks supports various [cluster types](https://docs.databricks.com/clusters/index.html):

* All-purpose clusters
* Job clusters
* SQL Warehouses (Classic and Serverless)

Since `dbx` is a CLI tool for development and advanced Databricks workflows management,
and currently workflows could only be launched on all-purpose and job clusters, we'll focus on these two types in this doc.

## :material-lightning-bolt-circle: All-purpose clusters

All-purpose clusters are dedicated for **interactive** usage, e.g.:

* exploration of the datasets using Notebooks
* development of new ETL pipelines
* interactive ML model development
* Multiple users might use the same cluster at the same time

With the description above, it becomes clear that all-purpose clusters are very suitable for the development loop,
and not really suitable for automated and scheduled workflow launches.

Therefore, with `dbx` we recommend using all-purpose clusters in the following cases:

* with `dbx execute` command when developing Python-based workflows
* Directly via Notebooks when using `dbx sync repo` together with Notebook-based development

!!! tip

    It's **not recommended to use all-purpose clusters** for any kind of **automated workflow deploy and launch**.
    For such cases, use job clusters - by this you'll ensure proper resource isolation and independence of any other activities of other users on the all-purpose clusters.

!!! danger ":material-package-variant: Python package-based projects might lead to undefined behaviour when launched as a workflow on all-purpose clusters"

    We **don't recommend** using `dbx deploy` and `dbx launch` with Python package-based projects on top of all-purpose clusters.<br/>

    If you're using a Python package-based project, **don't use** `existing_cluster_id` or `existing_cluster_name`
    properties in [:material-file-code: deployment file](../reference/deployment.md).<br/>

    It will lead to undefined behaviour during library installation and your job won't be launched if the library is already installed,
    **even if you provide a newer version of the package**.

    If you're going to develop such a project, please use the development loop described in [this documentation](../guides/python/devloop/package.md).

!!! danger ":material-language-java: JVM projects might lead to undefined behaviour when launched as a workflow on all-purpose clusters"

    Same reasoning as described for Python package-based projects is applied to JVM-based projects.
    Due to the library reinstallation issues, it's also not possible to run JVM-based projects from IDE on all-purpose clusters with `dbx`.<br/>
    For such cases, please use the development loop described in [this doc](../guides/jvm/jvm_devloop.md).

## :octicons-zap-16: Job clusters

In contrast to **all-purpose clusters**, job clusters are dedicated for a specific task. When you launch a workflow (or an asset-based launch), the VMs are started.
No other user or workflow could use these VMs, job clusters of a given workflow are fully dedicated for tasks defined inside this workflow.

This makes **job clusters** an excellent choice for:

* production-stage workflow launches
* integration tests
* SLA-based workflows where delivery on-time is a critical factor

All settings of a job cluster for each specific workflow and task inside this workflow can be fully configured using [:material-file-code: deployment file](../reference/deployment.md).

!!! tip "Cluster Reuse in Databricks Jobs"

    Databricks jobs with Jobs API v2.1 supports the [Cluster Reuse](https://www.databricks.com/blog/2022/02/04/saving-time-and-costs-with-cluster-reuse-in-databricks-jobs.html) feature.
    This feature allows tasks to reuse the same cluster, which greatly reduces the overall job execution time.<br/>

!!! tip "Cluster Reuse in assets-based launches"

    Since Cluster Reuse feature is not supported in [assets-based launch](../features/assets.md), you can add a failsafe switch to automatically generate new clusters.<br/>
    In this case a separate job cluster will be created for each task when `dbx launch --from-assets` is used.
    Please read more on this functionality [assets-based launch](../features/assets.md#failsafe-behaviour-for-shared-job-clusters).

!!! tip "Using instance pools"

    If you would like to speedup the start time of a job, try out [:fontawesome-solid-microchip: instance pools](https://docs.databricks.com/clusters/instance-pools/index.html) feature.<br/>
    This is especially useful for running integration tests and any kind of repetative workflows that can reuse the same set of VMs.


## :fontawesome-solid-list-check: Summary

To sum up the cases and potential choices for :material-lightning-bolt-circle: all-purpose and :octicons-zap-16: job clusters with `dbx`, take a look at the list below:

* Developing a :material-notebook-outline: Notebook-based project without IDE?
    * Use **all-purpose cluster** for development loop. `dbx` is not required.
    * Use **job clusters** and `dbx deploy` together with `dbx launch` for automated workflows as [described here](../guides/python/devops/notebook.md).
* Developing a :material-language-python: Python-package based project in IDE?
    * Use **all-purpose cluster** for development loop via `dbx execute` as [described here](../guides/python/devloop/package.md).
    * Use **job clusters** and `dbx deploy` together with `dbx launch` for automated workflows as [described here](../guides/python/devops/package.md).
* Developing a :material-blender-outline: Mixed-mode project (Python-package based project in IDE together with Notebooks in Databricks)?
    * Use **all-purpose cluster** for development loop. Synchronize local files to Repo with Notebooks via [`dbx sync repo`](../reference/cli.md#dbx-sync-repo) as [described here](../guides/python/devloop/mixed.md).
    * Use **job clusters** and `dbx deploy` together with `dbx launch` for automated workflows as [described here](../guides/python/devops/mixed.md).
* Developing a :material-language-java: JVM-based project in IDE?
    * Use local tests and **job clusters** with [:fontawesome-solid-microchip: instance pools](https://docs.databricks.com/clusters/instance-pools/index.html) for development loop as [described here](../guides/jvm/jvm_devloop.md).
    * Use **job clusters** and `dbx deploy` together with `dbx launch` for automated workflows as [described here](../guides/jvm/jvm_devops.md).

