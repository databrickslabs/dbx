# :material-file-code: Deployment file reference

Deployment file is one of the most important files for `dbx` functionality.

It contains workflows definitions, as well as `build` configurations.

The following file extensions are supported:

- `.yml` and `.yaml` expect a YAML-like payload. You can enable inplace Jinja support as described [here](../features/jinja_support.md)
- `.json` expects a JSON-like payload. You can enable inplace Jinja support as described [here](../features/jinja_support.md)
- `.yml.j2` and `.yaml.j2` will expect a YAML-like payload with possible sections of [Jinja blocks](../features/jinja_support.md).
- `.json.j2` will expect a JSON-like payload with possible sections of [Jinja blocks](../features/jinja_support.md).

By default `dbx` commands will search for a `deployment.*` file in the `conf` directory of the project.
Alternatively, all commands that require a deployment file support passing it explicitly via `--deployment-file` option.


Typical layout of this file looks like this:

```yaml title="conf/deployment.yml"

build: #(1)
  python: "pip"

environments: #(2)
  default: #(3)
    workflows: #(4)
      - name: "workflow1" #(5)
        tasks:
          - task_key: "task1"
            # example task payload
            python_wheel_task:
              package_name: "some-pkg"
              entry_point: "some-ep"
```

1. Read more on the topic of build management [here](../features/build_management.md)
2. This section is **required**. Without it `dbx` won't be able to read the file.
3. This is the name of a specific environment. This environment shall exist in [project file](./project.md)
4. This section is **required**. Without it `dbx` won't be able to read the workflows definitions.
5. Workflow names shall be unique.

!!! tip

    As the project file, deployment file supports multiple environments.
    You can configure them by naming new environments under the `environments` section.

The `workflows` section of the deployment file fully follows the [Databricks Jobs API structures](https://docs.databricks.com/dev-tools/api/latest/jobs.html).

## Advanced package dependency management


By default `dbx` is heavily oriented towards Python package-based projects. However, for pure Notebook or JVM projects this might be not necessary.

Therefore, to disable the default behaviour of `dbx` which tries to add the Python package dependency, use the `deployment_config` section inside the task definition:

```yaml title="conf/deployment.yml" hl_lines="12-13"
# some code omitted
environments:
  default:
    workflows:
      - name: "workflow1"
        tasks:
          - task_key: "task1"
            python_wheel_task: #(1)
              package_name: "some-pkg"
              entry_point: "some-ep"
          - task_key: "task2" #(2)
            deployment_config:
                no_package: true
            notebook_task:
                notebook_path: "/some/notebook/path"
```

1. Standard Python package-based payload, the python wheel dependency will be added by default
2. In the notebook task, the Python package is not required since code is delivered together with the Notebook.
   Therefore, we disable this behaviour by providing this property.
