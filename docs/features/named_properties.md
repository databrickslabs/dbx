# :material-rename-box: Name-based properties referencing

## :material-cog-stop: Legacy approach

!!! danger "This approach is considered legacy"

    Please don't use the approach described here. Use the new approach described below on the same page.

With `dbx` you can use name-based properties instead of providing ids in
the [:material-file-code: deployment file](../reference/deployment.md).

The following properties are supported:

- :material-state-machine: `existing_cluster_name` will be automatically replaced with `existing_cluster_id`
- :fontawesome-solid-microchip: `new_cluster.instance_pool_name` will be automatically replaced
  with `new_cluster.instance_pool_id`
- :fontawesome-solid-microchip: `new_cluster.driver_instance_pool_name` will be automatically replaced
  with `new_cluster.driver_instance_pool_id`
- :material-aws: `new_cluster.aws_attributes.instance_profile_name` will be automatically replaced
  with `new_cluster.aws_attributes.instance_profile_arn`
- :material-list-status: `new_cluster.policy_name` will automatically fetch all the missing policy parts and properly
  resolved them, replacing the `policy_name` with `policy_id`

By this simplification, you don't need to look-up for these id-based properties, you can simply provide the names.

!!! warning "Name verification"

    `dbx` will automatically check if the provided name exists and is unique, and if it's doesn't or it's non-unique you'll get an exception.

!!! danger "DLT support"

    Please note that `*_name`-based legacy properties **will not work** with DLT. Use the reference-based approach described below.

## :material-vector-link: Reference-based approach

<img src="https://img.shields.io/badge/available%20since-0.8.0-green?style=for-the-badge" alt="Available since 0.8.0"/>

With the new approach introduced in 0.8.0, we've made the named parameter passing way easier.

Simply use the string prefixed by the object type to create a reference which will be automatically replaced during
deployment.

General format for a reference looks line this:

```
object-type://object-name
```

The following references are supported:

| Reference prefix            | Referencing target        | API Method used for reference resolution                                                                                          |
|-----------------------------|---------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `instance-pool://`          | Instance Pools            | [ListInstancePools](https://docs.databricks.com/dev-tools/api/latest/instance-pools.html#list)                                    |
| `instance-profile://`       | Instance Profiles         | [ListInstanceProfiles](https://docs.databricks.com/dev-tools/api/latest/instance-profiles.html#list)                              |
| `pipeline://`               | Delta Live Tables         | [ListPipelines](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-api-guide.html#list-pipelines)          |
| `service-principal://`      | Service Principals        | [GetServicePrincipals (for workspaces)](https://docs.databricks.com/dev-tools/api/latest/scim/scim-sp.html#get-service-principals) |
| `warehouse://`              | Databricks SQL Warehouses | [ListSqlWarehouses](https://docs.databricks.com/sql/api/sql-endpoints.html#list)                                                  |
| `query://`                  | Databricks SQL Queries    | [GetSqlQueries](https://docs.databricks.com/sql/api/queries-dashboards.html#operation/sql-analytics-get-queries)                  |
| `dashboard://`              | Databricks SQL Dashboards | [GetSqlDashboards](https://docs.databricks.com/sql/api/queries-dashboards.html#operation/get-sql-analytics-dashboards)            |
| `alert://`                  | Databricks SQL Alerts     | [GetSqlAlerts](https://docs.databricks.com/sql/api/queries-dashboards.html#operation/databricks-sql-get-alerts)                   |
| `cluster-policy://`         | Cluster Policies          | [ListClusterPolicies](https://docs.databricks.com/dev-tools/api/latest/policies.html#operation/list-cluster-policies)             |
| `file://` or `file:fuse://` | Files                     | Please refer to the [file references documentation](./file_references.md)                                                         |

The provided object references are expected to be **unique**. If the name of the object is not unique, an error will be
raised.

## :material-list-status: Cluster policies resolution

[Cluster policies](https://docs.databricks.com/administration-guide/clusters/policies.html) is a very convenient
interface that allows generalizing specific rules to a wide set of clusters.

`dbx` provides capabilities to reference the policy name in the cluster definition, and some of the policy
properties will be automatically added to the cluster definition during deployment step.

### :material-format-list-checks: Resolution logic for properties

Please note that cluster policies are only resolved in the following cases:

- `policy_id` OR `policy_name` (latter is legacy) are provided as a part of `new_cluster` definition
- `policy_id` startswith `cluster-policy://`

The following logic is then applied to the policy and cluster definition:

1. Policy definition is traversed and transformed into Jobs API compatible format. Only the `fixed` properties are
   selected during traversal.
2. Policy definition deeply updates the cluster definition. If there are any keys provided in the cluster definition
   that are fixed in the policy, an error will be thrown.
3. Updated cluster definition goes back to the overall workflow definition

!!! warning "Other policy elements"

    `dbx` doesn't resolve and verify any other policy elements except the [Fixed ones](https://docs.databricks.com/administration-guide/clusters/policies.html#fixed-policy).

    Therefore, if you have for instance:

    * [Forbidden Policies](https://docs.databricks.com/administration-guide/clusters/policies.html#forbidden-policy)
    * [Limiting Policies](https://docs.databricks.com/administration-guide/clusters/policies.html#limiting-policies-common-fields)
    * [Allowlist Policies](https://docs.databricks.com/administration-guide/clusters/policies.html#allow-list-policy)

    They will only be resolved during the workflow deployment API call.

### :material-script: Init scripts resolution logic

<img src="https://img.shields.io/badge/available%20since-0.8.0-green?style=for-the-badge" alt="Available since 0.8.0"/>

[Init scripts](https://docs.databricks.com/clusters/init-scripts.html) is a powerful tool in Databricks to setup the
workflow environment before the workflow is running.

A very common use case is
to [setup the Python pip.conf](https://kb.databricks.com/en_US/clusters/install-private-pypi-repo)
if the workflow needs some private packages, then you don't need to declare it in
each [pip install](https://docs.databricks.com/libraries/notebooks-python-libraries.html#install-a-private-package-with-credentials-managed-by-databricks-secrets-with-pip)
.

To properly resolve init scripts together with the policy settings, dbx will merge in order and with deduplication the
init scripts from the cluster policy and those from the key `new_cluster.init_scripts`.

For instance, assuming there is a policy `policy-with-pip-install-script` that enforces adding an `init_script`:
```json title="cluster-policy.json"
{
  "init_scripts.0.dbfs.destination": {
    "type": "fixed",
    "value": "dbfs://some/path/script.sh"
  }
}
```

With the following [:material-file-code: deployment file](../reference/deployment.md) that references this policy:

```yaml title="conf/deployment.yml" linenums="1" hl_lines="8 12"
# irrelevant parts are omitted
environments:
  default:
    workflows:
      - name: workflow_name
        job_clusters:
        - new_cluster:
            policy_id: "cluster-policy://policy-with-pip-install-script"
            init_scripts:
            - dbfs:
                destination: dbfs:/some/path/install_sql_driver.sh
        tasks:
         ...
```

`dbx` will correctly resolve the `init_scripts` array and turn it into the following definition:

```json title="playload-for-api.json"
{
    "policy_id": "AAABBBCCCDDDD",
    "init_scripts": [
        {
            "dbfs": {
                "destination": "dbfs://some/path/script.sh"
            }
        },
        {
            "dbfs": {
                "destination": "dbfs:/some/path/install_sql_driver.sh"
            }
        }
    ]
}
```
