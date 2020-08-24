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

- As a first step, you need to create a project from a template. You can use your own template, or you can choose from existing templates:

```bash
cookiecutter --no-input https://github.com/databrickslabs/cicd-templates.git project_name="sample"
cd sample
```
- After creating a project, initialize `dbx` inside a directory. Provide any project name as a parameter:

```bash
dbx init --project-name="sample"
```
- Now, it's time to configure environments. As an example, create a new environment via given command:

```bash
dbx configure \
    --name="test" \
    --profile="some-profile-name" \
    --workspace-dir="/dbx/projects/sample"
```

This will configure a storage for project, and MLflow storage for deployment tracking.

- Next step would be to configure your deployment objects. To make this process easy and flexible, we're using jsonnet format. Please take a look on `.dbx/deployment.jsonnet` file for further configurations.
- After you've configured the `deployment.jsonnet` file, it's time to perform an actual deployment:
```bash
dbx deploy --environment=test 
```

The main idea of `.dbx/deployment.jsonnet` is to provide a flexible way to configure job with a lot of dependencies.
 
- Finally, after deploying all your job-related files, you launch the job via the following command:
```bash
dbx launch --environment=test --job=<any-job-name-provided in deployment.jsonnet>
```

## Dev documentation and notes

To launch `dbx` tests from a local machine, please prepare two profiles via `databricks configure`: `dbx-dev-aws` and `dbx-dev-azure`.
