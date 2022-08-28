# :shinto_shrine: Jinja support

[Jinja](https://jinja.palletsprojects.com/en/3.1.x/) is an extremely extensible engine that can add dynamic templating and :material-car-turbocharger: supercharge your static [:material-file-code: deployment file](../reference/deployment.md).


## :material-file-eye: File naming conventions

By default `dbx` will only recognize Jinja blocks inside deployment file if it has a `.j2` extension at the end, for example:

- `conf/deployment.json.j2` for JSON-formatted files
- `conf/deployment.yml.j2` for YAML-formatted files

## :material-toggle-switch: Enable inplace Jinja

If you would like Jinja to be recognized inside the standard file names (e.g. `conf/deployment.yml`), you can enable this at the project level by running:

```bash
dbx configure --enable-inplace-jinja-support
```

## :material-contain: Support for includes

Jinja support `include` clause which allows you to re-share common bits of configuration across multiple files and improve modularity of configurations.

Here is a quick example for JSON-based file:

```json title="conf/deployment.json.j2" hl_lines="7"
{
    "environments": {
        "default": {
            "jobs": [
                {
                    "name": "your-job-name",
                    "new_cluster": {% include 'includes/cluster-test.json.j2' %},
                    "libraries": [],
                    "max_retries": 0,
                    "spark_python_task": {
                        "python_file": "file://placeholder_1.py"
                    }
                }
            ]
        }
    }
}
```

Where the `includes` file contains:
```json
{
    "spark_version": "some-version",
    "node_type_id": "some-node-type",
    "aws_attributes": {
        "first_on_demand": 0,
        "availability": "SPOT"
    },
    "num_workers": 2
}
```

## :octicons-command-palette-24:  Environment variables

`dbx` supports passing environment variables into the deployment configuration, giving you an additional level of flexibility.

You can pass environment variables both into JSON and YAML-based configurations which are written in Jinja2 template format.
This allows you to parametrize the deployment and make it more flexible.

You can reach the `env` variables by using `env['VAR_NAME']` syntax, for example:
```yaml title="conf/deployment.yml"
# only relevant block shown
environments:
  default:
    - name: "job-with-tags"
      tags:
       - job_group: "{{ env['JOB_GROUP'] }}"
```

## :octicons-file-code-24: Variables file

In addition to the environment passing, you can also add a variables file option that will read the specified file:

```bash
dbx deploy --jinja-variables-file=conf/vars.yml
```
Imagine you have the following variables file:

```yaml title="conf/vars.yml"
TASK_CLUSTER:
    MIN_WORKERS: 1
    MAX_WORKERS: 5
TASK_NAME: 'main'
```

!!! warning

    Currently, only YAML-formatted variables file is supported.


In the deployment file these variables can be referenced in the following way:
```yaml title="conf/deployment.yml" hl_lines="10-12"
# irrelevant config parts are omitted
environments:
  default:
    workflows:
      - name: "charming-aurora-sample-etl"
        tasks:
          - task_key: "{{ var['TASK_NAME'] }}"
            new_cluster:
                autoscale:
                    min_workers: {{ var['TASK_CLUSTER']['MIN_WORKERS'] }}
                    max_workers: {{ var['TASK_CLUSTER']['MAX_WORKERS'] }}
```

!!! tip

    Variables file option `--jinja-variables-file` is supported in `dbx execute`, `dbx deploy` and `dbx launch`.

Variables from file can be used together with environment variables and any other Jinja features.

## :material-function: Built-in functions

In addition to the features above, `dbx` also provides set of functions that could be used inside Jinja files.

Currently, the following functions are supported:

* `dbx.get_last_modified_file(path, extension)`<br/>
  returns path to the last modified file from a path with a given extension.<br/>
  Path should be provided as a string, extension should be provided without a dot, e.g. `jar`.


## :material-function-variant: Custom functions support

!!! danger

    This functionality is experimental, use it with caution.<br/>

If you would like to implement a custom function for your Jinja templates, you can provide them into `.dbx/_custom_jinja_functions.py`
and then call from the deployment file by `custom.<function_name>` prefix, for example:

```python title=".dbx/_custom_jinja_functions.py"
def multiply_by_two(x: int) -> int:
    return x * 2
```

```yaml title="conf/deployment.yml" hl_lines="11-12"
# irrelevant config parts are omitted
environments:
  default:
    workflows:
      - name: "charming-aurora-sample-etl"
        tasks:
          - task_key: "some-task"
            new_cluster:
                autoscale:
                    min_workers: 1
                    max_workers: {{ custom.multiply_by_two(2) }}
```

