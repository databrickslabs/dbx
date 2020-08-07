# dbx by Databricks Labs

DataBricks eXtensions - aka `dbx` is a project, developed by Databricks Labs to  provide functionality for rapid development lifecycle on Databricks platform.  

This project shall be treated as an **extension** to the existing Databricks CLI.

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
