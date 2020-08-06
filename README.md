# dbx by Databricks Labs

DataBricks eXtensions - aka `dbx` is project provides functionality for rapid development lifecycle on Databricks platform.  

This project shall be treated as an extension to the existing Databricks CLI.

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
  --cloud=["Azure","AWS"] \
  --pipeline-engine=["GithubActions", "AzurePipelines"] \
  --project-name="my-project"
```


### Launch some existing code on the cluster
```bash
dbx launch \
  --dir=/some/path \
  --py-file=/some/pipeline.py \
  --cluster="some-cluster-name" \
  --trace
```


