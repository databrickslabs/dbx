# :material-link-plus: File reference management

During the workflow deployment, you frequently would like to upload some specific files to `DBFS` and reference them in
the workflow definition.

Any keys referenced in the deployment file starting with `file://` or `file:fuse://` will be uploaded to the artifact
storage.

References are resolved with relevance to the root of the project.

Imagine the following project structure:

```shell title="project structure"
.
├── charming_aurora #
│   ├── __init__.py
│   ├── common.py
│   └── tasks
│       ├── __init__.py
│       ├── sample_etl_task.py
│       └── sample_ml_task.py
├── conf
│   ├── deployment.yml
│   └── tasks
│       ├── sample_etl_config.yml
│       └── sample_ml_config.yml
```

In your [:material-file-code: deployment file](../reference/deployment.md) you can specify the file references in the
following way:

```yaml title="conf/deployment.yml" hl_lines="11-14"
environments:
  default:
    workflows:
      - name: "charming-aurora-sample-multitask"
        job_clusters:
          - job_cluster_key: "default"
            <<: *basic-static-cluster
        tasks:
          - task_key: "etl"
            job_cluster_key: "default"
            spark_python_task:
              python_file: "file://charming_aurora/tasks/sample_etl_task.py"
              parameters: [ "--conf-file", "file:fuse://conf/tasks/sample_etl_config.yml" ]
```

As you can see there are two **different** prefixes for file references.

There are two types of how the file path will be resolved and referenced in the final deployment definition:

=== "Standard"

    This definition looks like this `file://some/path/in/project/some.file`.<br/>
    It will be resolved into `dbfs://<artifact storage prefix>/some/path/in/project/some.file`.

=== "FUSE"

    This definition looks like this `file:fuse://some/path/in/project/some.file`.<br/>
    It will be resolved into `/dbfs/<artifact storage prefix>/some/path/in/project/some.file`<br/>.

The latter type of path resolution might come in handy when the using system doesn't know how to work with cloud storage protocols.
