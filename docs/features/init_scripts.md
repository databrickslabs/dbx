# :material-script: Init scripts

[Init scripts](https://docs.databricks.com/clusters/init-scripts.html) is a powerful tool in Databricks to setup the workflow environment before the workflow is running. A very common use case is to [setup the Python pip.conf](https://learn.microsoft.com/en-us/azure/databricks/kb/clusters/install-private-pypi-repo) if the workflow needs some private packages, then you don't need to declare it in each [pip install](https://docs.databricks.com/libraries/notebooks-python-libraries.html#install-a-private-package-with-credentials-managed-by-databricks-secrets-with-pip).

## Appending custom init scripts

<img src="https://img.shields.io/badge/available%20since-0.8.0-green?style=for-the-badge" alt="Available since 0.8.0"/>

In the dbx deployment file, if the key `new_cluster.policy_name` is specified, and the policy contains some init scripts. You can **append** additional init scripts by declaring them in the key `new_cluster.init_scripts`, and enable the option by `dbx configure --enable-custom-init-scripts`. Then during `dbx deploy`, dbx will merge in order and with deduplication, the init scripts from the cluster policy and those from the key `new_cluster.init_scripts`.

One of the use cases is that as job cluster with [cluster pool](https://docs.databricks.com/clusters/instance-pools/index.html) is an isolated environment, two workflows sharing the the same pool won't be never distributed to the same node, we can define some shared init scripts in the cluster policy, such as `setup_pip.conf.sh`, etc., and we enable users to declare their own init scripts in the deployment file, for example `install_sql_driver.sh`. By this way, we keep the init scripts management easier instead of declaring all in the cluster policies.

Here is a quick example how to use it:

```bash
# the file .dbx/project.json must exist before enabling this feature
dbx configure
dbx configure --enable-custom-init-scripts
```

!!! tip
    Under the hook, the flag sets the key `custom_init_scripts` to `true` in the file `.dbx/project.json`. Without this flag, the value is set to `false` by default.

And hereunder a simplified example of the [:material-file-code: deployment file](../reference/deployment.md):

```yaml title="conf/deployment.yml" linenums="1" hl_lines="7 9"
environments:
  default:
    workflows:
      - name: workflow_name
        job_clusters:
        - new_cluster:
            policy_name: pip.conf
            instance_pool_name: pool_name
            init_scripts:
            - dbfs:
                destination: dbfs:/{path_to_the_custom_init_scripts}/install_sql_driver.sh
        tasks: []
```

The final workflow definition compiled by `dbx deploy` will be:

```json title="simplified final workflow definition"
...
"job_clusters": [
    {
        "new_cluster": {
            ...
            "init_scripts": [
                {
                    "dbfs": {
                        "destination": "dbfs:/{path_to_the_cluster_policy_init_scripts}/setup_pip.conf.sh"
                    }
                },
                {
                    "dbfs": {
                        "destination": "dbfs:/{path_to_the_custom_init_scripts}/install_sql_driver.sh"
                    }
                }
            ],
            "instance_pool_id": "...",
            "policy_id": "...",
            ...
        }
    }
],
...
```
