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
  --project-name="my-project" \
  --cloud=["Azure","AWS"] \
  --pipeline-engine=["GitHub Actions", "Azure Pipelines"] 
```


