# :fontawesome-solid-users-gear: Permissions management

`dbx` supports permissions management both for Jobs API 2.0 and Jobs API 2.1.

!!! tip

    You can find the full specification for Permissions API [here](https://docs.databricks.com/dev-tools/api/latest/permissions.html).


## :material-file-check: Providing the permissions

To manage permissions for Jobs API 2.1, provide the following payload at the workflow level:

```yaml
environments:
  default:
    workflows:
      - name: "some-workflow"
        access_control_list:
          - user_name: "some_user@example.com"
            permission_level: "IS_OWNER"
          - group_name: "some-user-group"
            permission_level: "CAN_VIEW"
```

Please note that the permissions **must be exhaustive**.
It means that per each workflow at least the owner (`permission_level: "IS_OWNER"`) shall be specified (even if it's already specified in the UI).

!!! tip "Managing permissions for service principals"

    Take a look at [this example](../reference/deployment.md#managing-the-workflow-as-a-service-principal) in the deployment file reference.
