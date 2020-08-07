# {{cookiecutter.project_name}}

This is a sample project created by `dbx` - DataBricks eXtensions module. 


  
## Pre-requisites 

Please create profiles for all Databricks Workspaces which you're going to use in your project via:
```
databricks configure --profile {profile_name} --token
```

By default, `dbx` uses your default profile, configured by Databricks CLI. You can provide alternative profile by using `--profile` option.
 

## Create dev cluster

To create development cluster, please edit cluster configurations in `config/dev/cluster.json` folder. 
After you've configured the cluster, please create it via command:
```
dbx create-dev-cluster --profile=<your profile name here> 
```
Cluster id will be written into `.dbx.lock.json` file.

## Execute your jobs on dev cluster

This option is designed to execute job in an interactive mode from local machine.

```
dbx execute <job-name>
```
