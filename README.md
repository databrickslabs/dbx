# dbx by Databricks Labs

DataBricks eXtensions - aka `dbx` is a project, developed by Databricks Labs to  provide functionality for rapid development lifecycle on Databricks platform.  

This project shall be treated as an **extension** to the existing Databricks CLI.

## Project Support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). 
They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support.

## Installation

Via `pip`:
```
pip install dbx
```

Via `conda`:
```
conda install dbx
```

### Initialize the project
```bash
dbx init \
  --project-name=dbx_project \
  --cloud=["Azure","AWS"] \
  --pipeline-engine=["GitHub Actions", "Azure Pipelines"]

cd dbx_project
```

### Create your dev cluster

```bash
dbx create-dev-cluster
```

### Add your job into project

Create a new job under `jobs` directory. Write main executable code in the `entrypoint.py` file.

### Launch your code on dev

```bash
dbx execute --job-name=<your-job-name>
```
As soon as cluster launch happens, you could dynamically change your code and execute it.


## Dev documentation and notes


To launch `dbx` tests from a local machine, please prepare two profiles via `databricks configure`: `dbx-dev-aws` and `dbx-dev-azure`.

## Compatibility with cicd-templates

Important point of `dbx` is to provide compatible interfaces with CICD pipelines for any users who used them before. 
To do so, we provide the following migration patterns for `cicd-templates`:

| cicd-templates                                                                             | dbx                                                                                                 |
|--------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| ./create_cluster <some-dir> <some-pipeline> --cluster-id=<some-id>                         | dbx legacy create-cluster <some-dir> <some-pipeline> --cluster-id=<some-id>                         |
| ./create_cluster <some-dir> <some-pipeline> --new-cluster                                  | dbx legacy create-cluster <some-dir> <some-pipeline> --cluster-id=<some-id>                         |
| ./run_now <some-dir> <some-pipeline> --cluster=<some-id> --force-new-context --any-cluster | dbx legacy run-now <some-dir> <some-pipeline> --cluster=<some-id> --force-new-context --any-cluster |
| ./run_pipeline <dir-with-pipelines> --pipeline-name                                        | dbx legacy run-pipeline <dir-with-pipelines> --pipeline-name                                        |

Please consider that these options are provided **only** for compatibility reasons. We recommend to use `dbx` only with `dbx`-based projects.