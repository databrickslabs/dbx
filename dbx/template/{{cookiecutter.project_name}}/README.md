# {{cookiecutter.project_name}}

This is a sample project created by `dbx` - DataBricks eXtensions module. 

## Pre-requisites 

Please create profiles for all Databricks Workspaces which you're going to use in your project via:

```
databricks configure --profile {profile_name} --token
```


## Create dev cluster

To create development cluster, please edit cluster configurations in `clusters` folder. 
After you've configured the cluster, please create it via command:
```
databricks clusters create \
    --json-file=clusters/<your-cluster-name>.json \
    --profile=<your profile name here> 
```

## Execute your jobs on dev cluster

If you would like to use code from some set of directories, please use:
```
dbx launch \
    --dir=/some/python/code \
    --dir=/some/other/python/code \
    --py-file=/path/to/file \
    
```