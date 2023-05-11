# :fontawesome-solid-users-gear: Permissions management

`dbx` supports permissions management for Jobs API :material-surround-sound-2-0: and Jobs API :material-surround-sound-2-1: as well as for workflows in the format of :material-table-heart: Delta Live Tables.

!!! tip

    You can find the full specification for Permissions API [here](https://docs.databricks.com/dev-tools/api/latest/permissions.html).


## :material-file-check: Providing the permissions

!!! warning "Enforcing Jobs API 2.1 usage to work with ACLs"

    Provisioning of ACLs is only supported with Jobs API 2.1. To enforce Jobs API 2.1 usage, please use the following settings:

    For local setup via `~/.databrickscfg` use the following command:

    ```bash
    databricks jobs configure --version=2.1 # add --profile=<profile-name> to enforce configuration for different profiles
    ```

    In CI pipelines, provide the following environment variable:

    ```bash
    DATABRICKS_JOBS_API_VERSION=2.1
    ```

To manage permissions provide the following payload at the workflow level:

```yaml
environments:
  default:
    workflows:
      # example for DLT pipeline
      - name: "some-dlt-pipeline"
        libraries:
          - notebook:
              path: "/some/repos"
        access_control_list:
          - user_name: "some_user@example.com"
            permission_level: "IS_OWNER"
          - group_name: "some-user-group"
            permission_level: "CAN_VIEW"

      # example for multitask workflow
      - name: "some-workflow"
        tasks:
          ...
        access_control_list:
          - service_principal_name: "service-principal://some-sp-name" # alternatively, you can directly provide the Id string itself
            permission_level: "IS_OWNER"
          - user_name: "some_user@example.com"
            permission_level: "CAN_MANAGE"
          - group_name: "some-user-group"
            permission_level: "CAN_VIEW"
```

Please note that the permissions **must be exhaustive**.
It means that per each workflow at least the owner (`permission_level: "IS_OWNER"`) shall be specified (even if it's already specified in the UI).

!!! tip "Managing permissions for service principals"

    Take a look at [this example](../reference/deployment.md#managing-the-workflow-as-a-service-principal) in the deployment file reference.
